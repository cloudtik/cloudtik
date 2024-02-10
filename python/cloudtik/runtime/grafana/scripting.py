import os
from shlex import quote

from cloudtik.core._private.util.core_utils import exec_with_output, http_address_string
from cloudtik.core._private.runtime_factory import BUILT_IN_RUNTIME_PROMETHEUS, BUILT_IN_RUNTIME_GRAFANA
from cloudtik.core._private.util.runtime_utils import get_runtime_config_from_node, get_runtime_value, \
    save_yaml, get_runtime_node_ip, get_runtime_head_host, get_runtime_cluster_name
from cloudtik.core._private.service_discovery.utils import \
    serialize_service_selector, get_service_selector_copy
from cloudtik.runtime.common.utils import stop_pull_service_by_identifier
from cloudtik.runtime.grafana.utils import _get_config, GRAFANA_DATA_SOURCES_CONFIG_KEY, \
    GRAFANA_DATA_SOURCES_SCOPE_LOCAL, get_data_source_name, get_prometheus_data_source, _get_home_dir, \
    _get_service_port, GRAFANA_DATA_SOURCES_SERVICES_CONFIG_KEY, _get_logs_dir

GRAFANA_PULL_DATA_SOURCES_INTERVAL = 30


###################################
# Calls from node when configuring
###################################


def configure_data_sources(head):
    runtime_config = get_runtime_config_from_node(head)
    grafana_config = _get_config(runtime_config)

    data_sources = grafana_config.get(GRAFANA_DATA_SOURCES_CONFIG_KEY)
    if data_sources is None:
        data_sources = []

    data_sources_scope = get_runtime_value("GRAFANA_DATA_SOURCES_SCOPE")
    prometheus_port = get_runtime_value("GRAFANA_LOCAL_PROMETHEUS_PORT")
    if data_sources_scope == GRAFANA_DATA_SOURCES_SCOPE_LOCAL and prometheus_port:
        # add a local data resource for prometheus
        # use cluster_name + service_name as the data source name
        name = get_data_source_name(
            BUILT_IN_RUNTIME_PROMETHEUS,
            get_runtime_cluster_name())
        head_host = get_runtime_head_host(head)
        url = http_address_string(head_host, prometheus_port)
        prometheus_data_source = get_prometheus_data_source(
            name, url, is_default=True)
        data_sources.append(prometheus_data_source)

    if data_sources:
        _save_data_sources_config(data_sources)


def _save_data_sources_config(data_sources):
    # write the data sources file
    home_dir = _get_home_dir()
    config_file = os.path.join(
        home_dir, "conf", "provisioning",
        "datasources", "static-data-sources.yaml")

    config_object = {
        "apiVersion": 1,
        "datasources": data_sources
    }
    save_yaml(config_file, config_object)


def _get_service_identifier():
    return "{}-discovery".format(BUILT_IN_RUNTIME_GRAFANA)


def _get_admin_api_endpoint(node_ip, grafana_port):
    return http_address_string(
        node_ip, grafana_port)


def start_pull_service(head):
    runtime_config = get_runtime_config_from_node(head)
    grafana_config = _get_config(runtime_config)
    grafana_port = _get_service_port(grafana_config)

    node_ip = get_runtime_node_ip()
    admin_api_endpoint = _get_admin_api_endpoint(node_ip, grafana_port)

    service_selector = get_service_selector_copy(
        grafana_config, GRAFANA_DATA_SOURCES_SERVICES_CONFIG_KEY)
    service_selector_str = serialize_service_selector(service_selector)

    service_identifier = _get_service_identifier()
    logs_dir = _get_logs_dir()

    cmd = ["cloudtik", "node", "service", service_identifier, "start"]
    cmd += ["--service-class=cloudtik.runtime.grafana.discovery.DiscoverDataSources"]
    cmd += ["--logs-dir={}".format(quote(logs_dir))]

    # job parameters
    cmd += ["interval={}".format(
        GRAFANA_PULL_DATA_SOURCES_INTERVAL)]
    cmd += ["admin_endpoint={}".format(quote(admin_api_endpoint))]
    if service_selector_str:
        cmd += ["service_selector={}".format(service_selector_str)]

    cmd_str = " ".join(cmd)
    exec_with_output(cmd_str)


def stop_pull_service():
    service_identifier = _get_service_identifier()
    stop_pull_service_by_identifier(service_identifier)

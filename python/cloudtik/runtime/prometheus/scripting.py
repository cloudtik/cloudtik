import os
from shlex import quote
from typing import Any, Dict

from cloudtik.core._private import constants
from cloudtik.core._private.util.core_utils import exec_with_output, get_list_for_update, get_address_string
from cloudtik.core._private.runtime_factory import BUILT_IN_RUNTIME_PROMETHEUS
from cloudtik.core._private.util.runtime_utils import load_and_save_yaml, \
    get_runtime_config_from_node, save_yaml, get_runtime_value, get_runtime_head_host, get_runtime_node_address_type, \
    get_runtime_workspace_name, get_runtime_cluster_name
from cloudtik.core._private.service_discovery.utils import \
    SERVICE_DISCOVERY_PORT, \
    SERVICE_SELECTOR_SERVICES, SERVICE_SELECTOR_TAGS, SERVICE_SELECTOR_LABELS, SERVICE_SELECTOR_EXCLUDE_LABELS, \
    SERVICE_SELECTOR_RUNTIMES, SERVICE_SELECTOR_CLUSTERS, SERVICE_DISCOVERY_LABEL_RUNTIME, \
    SERVICE_DISCOVERY_LABEL_CLUSTER, SERVICE_SELECTOR_EXCLUDE_JOINED_LABELS, SERVICE_SELECTOR_SERVICE_TYPES, \
    SERVICE_DISCOVERY_LABEL_SERVICE
from cloudtik.runtime.common.utils import stop_pull_service_by_identifier
from cloudtik.runtime.prometheus.utils import PROMETHEUS_SERVICE_DISCOVERY_CONSUL, \
    PROMETHEUS_SCRAPE_SERVICES_CONFIG_KEY, _get_config, PROMETHEUS_SCRAPE_SCOPE_WORKSPACE, _get_home_dir, \
    PROMETHEUS_SCRAPE_SCOPE_FEDERATION, PROMETHEUS_SERVICE_DISCOVERY_FILE, _get_federation_targets, \
    PROMETHEUS_PULL_NODE_TYPES_CONFIG_KEY, PROMETHEUS_PULL_SERVICES_CONFIG_KEY, _get_logs_dir

PROMETHEUS_PULL_LOCAL_TARGETS_INTERVAL = 15

###################################
# Calls from node when configuring
###################################


def _get_config_file(scrape_scope):
    home_dir = _get_home_dir()
    if scrape_scope == PROMETHEUS_SCRAPE_SCOPE_WORKSPACE:
        config_file_name = "scrape-config-workspace-consul.yaml"
    elif scrape_scope == PROMETHEUS_SCRAPE_SCOPE_FEDERATION:
        config_file_name = "scrape-config-federation-consul.yaml"
    else:
        config_file_name = "scrape-config-local-consul.yaml"
    return os.path.join(home_dir, "conf", config_file_name)


def configure_scrape(head):
    runtime_config = get_runtime_config_from_node(head)
    prometheus_config = _get_config(runtime_config)

    sd = get_runtime_value("PROMETHEUS_SERVICE_DISCOVERY")
    scrape_scope = get_runtime_value("PROMETHEUS_SCRAPE_SCOPE")
    if sd == PROMETHEUS_SERVICE_DISCOVERY_CONSUL:
        # tags and labels only support service discovery based scrape (consul)
        service_selector = prometheus_config.get(
            PROMETHEUS_SCRAPE_SERVICES_CONFIG_KEY, {})
        services = service_selector.get(SERVICE_SELECTOR_SERVICES)
        service_types = service_selector.get(SERVICE_SELECTOR_SERVICE_TYPES)
        tags = service_selector.get(SERVICE_SELECTOR_TAGS)
        labels = service_selector.get(SERVICE_SELECTOR_LABELS)
        runtimes = service_selector.get(SERVICE_SELECTOR_RUNTIMES)
        clusters = service_selector.get(SERVICE_SELECTOR_CLUSTERS)
        exclude_labels = service_selector.get(SERVICE_SELECTOR_EXCLUDE_LABELS)
        exclude_joined_labels = service_selector.get(SERVICE_SELECTOR_EXCLUDE_JOINED_LABELS)

        if (services or service_types or
                tags or labels or
                runtimes or clusters or
                exclude_labels or exclude_joined_labels):
            config_file = _get_config_file(scrape_scope)
            _update_scrape_config(
                config_file,
                services, service_types,
                tags, labels,
                runtimes, clusters,
                exclude_labels, exclude_joined_labels)
    elif sd == PROMETHEUS_SERVICE_DISCOVERY_FILE:
        if scrape_scope == PROMETHEUS_SCRAPE_SCOPE_FEDERATION:
            federation_targets = _get_federation_targets(prometheus_config)
            _save_federation_targets(federation_targets)


def _add_label_match_list(scrape_config, label_name, values):
    # Drop targets doesn't belong any of these runtimes
    relabel_configs = get_list_for_update(scrape_config, "relabel_configs")
    match_values = "({})".format(
        "|".join(values)
    )
    relabel_config = {
        "source_labels": ["__meta_consul_service_metadata_{}".format(
            label_name)],
        "regex": match_values,
        "action": "keep",
    }
    relabel_configs.append(relabel_config)


def _update_scrape_config(
        config_file,
        services, service_types,
        tags, labels,
        runtimes, clusters,
        exclude_labels, exclude_joined_labels):
    def update_contents(config_object):
        scrape_configs = config_object["scrape_configs"]
        for scrape_config in scrape_configs:
            if services:
                # Any services in the list (OR)
                sd_configs = scrape_config["consul_sd_configs"]
                for sd_config in sd_configs:
                    # replace the services if specified
                    sd_config["services"] = services
            if tags:
                # Services must contain all the tags (AND)
                sd_configs = scrape_config["consul_sd_configs"]
                for sd_config in sd_configs:
                    base_tags = get_list_for_update(sd_config, "tags")
                    base_tags.append(tags)
            if labels:
                # Drop targets for which regex does not match the concatenated source_labels.
                # Any unmatch will drop (All the labels must match) (AND)
                relabel_configs = get_list_for_update(scrape_config, "relabel_configs")
                for label_key, label_value in labels.items():
                    relabel_config = {
                        "source_labels": ["__meta_consul_service_metadata_{}".format(
                            label_key)],
                        "regex": label_value,
                        "action": "keep",
                    }
                    relabel_configs.append(relabel_config)

            if service_types:
                _add_label_match_list(
                    scrape_config, SERVICE_DISCOVERY_LABEL_SERVICE, service_types)
            if runtimes:
                _add_label_match_list(
                    scrape_config, SERVICE_DISCOVERY_LABEL_RUNTIME, runtimes)
            if clusters:
                _add_label_match_list(
                    scrape_config, SERVICE_DISCOVERY_LABEL_CLUSTER, clusters)

            if exclude_labels:
                # Drop targets for which regex matches the concatenated source_labels.
                # Any match will drop (OR)
                relabel_configs = get_list_for_update(scrape_config, "relabel_configs")
                for label_key, label_value in exclude_labels.items():
                    relabel_config = {
                        "source_labels": ["__meta_consul_service_metadata_{}".format(
                            label_key)],
                        "regex": label_value,
                        "action": "drop",
                    }
                    relabel_configs.append(relabel_config)

            if exclude_joined_labels:
                # Drop targets for which regex matches the concatenated source_labels.
                # Match all the labels will drop (AND)
                relabel_configs = get_list_for_update(scrape_config, "relabel_configs")
                for joined_labels in exclude_joined_labels:
                    # all the labels must match for each joined labels
                    source_labels = []
                    label_values = []
                    for label_key, label_value in joined_labels.items():
                        source_labels.append("__meta_consul_service_metadata_{}".format(
                            label_key))
                        label_values.append(label_value)
                    joined_label_values = ";".join(label_values)
                    relabel_config = {
                        "source_labels": source_labels,
                        "separator": ';',
                        "regex": joined_label_values,
                        "action": "drop",
                    }
                    relabel_configs.append(relabel_config)

    load_and_save_yaml(config_file, update_contents)


def _save_federation_targets(federation_targets):
    home_dir = _get_home_dir()
    config_file = os.path.join(home_dir, "conf", "federation-targets.yaml")
    save_yaml(config_file, federation_targets)


def _get_service_identifier():
    return "{}-discovery".format(BUILT_IN_RUNTIME_PROMETHEUS)


def _get_pull_services_str(pull_services: Dict[str, Any]) -> str:
    # Format in a form like 'service-1:port1:node_type_1,...'
    pull_service_str_list = []
    for service_name, pull_service in pull_services.items():
        service_port = pull_service[SERVICE_DISCOVERY_PORT]
        node_types = pull_service[PROMETHEUS_PULL_NODE_TYPES_CONFIG_KEY]
        pull_service_str = ":".join([service_name, str(service_port)] + node_types)
        pull_service_str_list.append(pull_service_str)

    return ",".join(pull_service_str_list)


def start_pull_service(head):
    runtime_config = get_runtime_config_from_node(head)
    prometheus_config = _get_config(runtime_config)
    pull_services = prometheus_config.get(PROMETHEUS_PULL_SERVICES_CONFIG_KEY)

    service_identifier = _get_service_identifier()
    logs_dir = _get_logs_dir()

    redis_host = get_runtime_head_host(head)
    redis_address = get_address_string(redis_host, constants.CLOUDTIK_DEFAULT_PORT)
    workspace_name = get_runtime_workspace_name()
    cluster_name = get_runtime_cluster_name()
    address_type = get_runtime_node_address_type()

    cmd = ["cloudtik", "node", "service", service_identifier, "start"]
    cmd += ["--service-class=cloudtik.runtime.prometheus.discovery.DiscoverLocalTargets"]
    cmd += ["--logs-dir={}".format(quote(logs_dir))]

    # job parameters
    cmd += ["interval={}".format(
        PROMETHEUS_PULL_LOCAL_TARGETS_INTERVAL)]
    if pull_services:
        pull_services_str = _get_pull_services_str(pull_services)
        cmd += ["services={}".format(pull_services_str)]
    cmd += ["redis_address={}".format(redis_address)]
    cmd += ["redis_password={}".format(
        constants.CLOUDTIK_REDIS_DEFAULT_PASSWORD)]
    cmd += ["workspace_name={}".format(workspace_name)]
    cmd += ["cluster_name={}".format(cluster_name)]
    cmd += ["address_type={}".format(str(address_type))]

    cmd_str = " ".join(cmd)
    exec_with_output(cmd_str)


def stop_pull_service():
    service_identifier = _get_service_identifier()
    stop_pull_service_by_identifier(service_identifier)

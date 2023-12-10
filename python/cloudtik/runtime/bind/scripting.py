import os

from cloudtik.core._private.util.resolv_conf import get_resolv_conf_name_servers
from cloudtik.runtime.bind.utils import _get_home_dir


###################################
# Calls from node when configuring
###################################


def configure_upstream(head):
    conf_dir = os.path.join(
        _get_home_dir(), "conf")
    origin_resolv_conf = os.path.join(
        conf_dir, "resolv.conf")
    upstream_config_file = os.path.join(
        conf_dir, "named.conf.upstream")

    name_servers = get_resolv_conf_name_servers(
        origin_resolv_conf)
    with open(upstream_config_file, "w") as f:
        f.write('zone "." {\n')
        f.write('  type forward;\n')
        f.write('  forwarders {\n')
        for name_server in name_servers:
            f.write("    {};\n".format(name_server))
        f.write('  };\n')
        f.write('};\n')

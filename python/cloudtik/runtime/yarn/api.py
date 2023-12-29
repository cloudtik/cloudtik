"""IMPORTANT: this is an experimental interface and not currently stable."""

from typing import Union, Optional

from cloudtik.core.api import Cluster, ThisCluster
from cloudtik.runtime.yarn.utils import request_rest_yarn, \
    get_runtime_endpoints


class YARNCluster(Cluster):
    def __init__(
            self, cluster_config: Union[dict, str],
            should_bootstrap: bool = True,
            no_config_cache: bool = True,
            verbosity: Optional[int] = None,
            skip_runtime_bootstrap: bool = False) -> None:
        """Create a YARN cluster object to operate on with this API.

        Args:
            cluster_config (Union[str, dict]): Either the config dict of the
                cluster, or a path pointing to a file containing the config.
        """
        super().__init__(
            cluster_config, should_bootstrap,
            no_config_cache, verbosity,
            skip_runtime_bootstrap=skip_runtime_bootstrap)

    def yarn(self, endpoint: str):
        """Make a rest request to YARN Resource Manager

        Args:
            endpoint (str): The YARN resource manager rest endpoint to request
        """
        return request_rest_yarn(self.config, endpoint)

    def get_endpoints(self):
        return get_runtime_endpoints(self.config)


class ThisYARNCluster(ThisCluster):
    def __init__(self, verbosity: Optional[int] = None) -> None:
        """Create a YARN cluster object to operate on with this API on head."""
        super().__init__(verbosity)

    def yarn(self, endpoint: str):
        """Make a rest request to YARN Resource Manager

        Args:
            endpoint (str): The YARN resource manager rest endpoint to request
        """
        return request_rest_yarn(self.config, endpoint, on_head=True)

    def get_endpoints(self):
        return get_runtime_endpoints(self.config)

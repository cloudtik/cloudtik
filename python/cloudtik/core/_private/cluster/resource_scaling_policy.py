import logging
from typing import Optional

from cloudtik.core._private.cluster.scaling_policies import \
    _create_built_in_scaling_policy, _create_scaling_resources
from cloudtik.core._private.state.scaling_state import ScalingStateClient, ScalingState
from cloudtik.core._private.utils import \
    _get_runtime_scaling_policy, _get_scaling_policy_cls, _get_scaling_config

logger = logging.getLogger(__name__)


class ResourceScalingPolicy:
    def __init__(
            self,
            head_host,
            scaling_state_client: ScalingStateClient):
        self.head_host = head_host
        self.scaling_state_client = scaling_state_client
        self.config = None
        # Multiple scaling policies will cause confusion
        self.scaling_policy = None

    def reset(self, config):
        self.config = config
        # Reset is called when the configuration changed
        # Always recreate the scaling policy when config is changed
        # in the case that the scaling policy is disabled in the change
        self.scaling_policy = self._create_scaling_policy(self.config)
        if self.scaling_policy is not None:
            logger.info(
                f"CloudTik scaling with: {self.scaling_policy.name()}")
        else:
            logger.info(
                "CloudTik: No scaling policy is used.")

    def _create_scaling_policy(self, config):
        scaling_policy = _get_runtime_scaling_policy(config, self.head_host)
        if scaling_policy is not None:
            return scaling_policy

        # Check whether there are any user scaling policies configured
        user_scaling_policy = self._get_user_scaling_policy(config, self.head_host)
        if user_scaling_policy is not None:
            return user_scaling_policy

        # Check whether there are any built-in scaling policies configured
        system_scaling_policy = self._get_system_scaling_policy(config, self.head_host)
        if system_scaling_policy is not None:
            return system_scaling_policy

        return self._get_default_scaling_policy(config, self.head_host)

    def update(self):
        # Pulling data from resource management system
        scaling_state = self.get_scaling_state()
        if scaling_state is not None:
            self.scaling_state_client.update_scaling_state(
                scaling_state)

    def get_scaling_state(self) -> Optional[ScalingState]:
        if self.scaling_policy is None:
            return None
        return self.scaling_policy.get_scaling_state()

    def _get_user_scaling_policy(self, config, head_host):
        scaling_config = _get_scaling_config(config)
        if not scaling_config:
            return None
        if "scaling_policy_class" not in scaling_config:
            return None
        scaling_policy_cls = _get_scaling_policy_cls(
            scaling_config["scaling_policy_class"])
        return scaling_policy_cls(config, head_host)

    def _get_system_scaling_policy(self, config, head_host):
        scaling_config = _get_scaling_config(config)
        if not scaling_config:
            return None
        return _create_built_in_scaling_policy(config, head_host, scaling_config)

    def _get_default_scaling_policy(self, config, head_host):
        return _create_scaling_resources(config, head_host)

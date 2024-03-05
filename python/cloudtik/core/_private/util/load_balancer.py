

def get_load_balancer_config_name(load_balancer_config):
    return load_balancer_config["name"]


def get_load_balancer_config_type(load_balancer_config):
    return load_balancer_config["type"]


def get_load_balancer_config_scheme(load_balancer_config):
    return load_balancer_config["scheme"]


def get_load_balancer_public_ips(load_balancer_config):
    return load_balancer_config.get("public_ips", [])


def get_load_balancer_service_groups(load_balancer_config):
    return load_balancer_config.get("service_groups", [])


def get_service_group_services(service_group):
    return service_group.get("services", [])


def get_service_group_listeners(service_group):
    return service_group.get("listeners", [])


def get_service_targets(service):
    return service.get("targets", [])

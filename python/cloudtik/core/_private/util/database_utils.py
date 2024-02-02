
DATABASE_CONFIG_ENGINE = "engine"
DATABASE_CONFIG_ADDRESS = "address"
DATABASE_CONFIG_PORT = "port"
DATABASE_CONFIG_USERNAME = "username"
DATABASE_CONFIG_PASSWORD = "password"
DATABASE_CONFIG_DATABASE = "database"

DATABASE_ENGINE_MYSQL = "mysql"
DATABASE_ENGINE_POSTGRES = "postgres"

DATABASE_ENV_ENABLED = "SQL_DATABASE"
DATABASE_ENV_ENGINE = "SQL_DATABASE_ENGINE"
DATABASE_ENV_HOST = "SQL_DATABASE_HOST"
DATABASE_ENV_PORT = "SQL_DATABASE_PORT"
DATABASE_ENV_USERNAME = "SQL_DATABASE_USERNAME"
DATABASE_ENV_PASSWORD = "SQL_DATABASE_PASSWORD"

DATABASE_PORT_MYSQL_DEFAULT = 3306
DATABASE_USERNAME_MYSQL_DEFAULT = "root"
DATABASE_PASSWORD_MYSQL_DEFAULT = "cloudtik"

DATABASE_PORT_POSTGRES_DEFAULT = 5432
DATABASE_USERNAME_POSTGRES_DEFAULT = "cloudtik"
DATABASE_PASSWORD_POSTGRES_DEFAULT = "cloudtik"


def get_database_engine(database_config):
    engine = database_config.get(DATABASE_CONFIG_ENGINE)
    return get_validated_engine(engine)


def get_validated_engine(engine):
    if engine and engine != DATABASE_ENGINE_MYSQL and engine != DATABASE_ENGINE_POSTGRES:
        raise ValueError(
            "The database engine type {} is not supported.".format(engine))
    return engine or DATABASE_ENGINE_MYSQL


def get_database_address(database_config):
    return database_config.get(DATABASE_CONFIG_ADDRESS)


def get_database_port(database_config):
    port = database_config.get(DATABASE_CONFIG_PORT)
    if port:
        return port
    engine = get_database_engine(database_config)
    return get_database_default_port(engine)


def get_database_default_port(engine):
    return (DATABASE_PORT_MYSQL_DEFAULT
            if engine == DATABASE_ENGINE_MYSQL
            else DATABASE_PORT_POSTGRES_DEFAULT)


def get_database_username(database_config):
    return database_config.get(
        DATABASE_CONFIG_USERNAME)


def get_database_username_with_default(database_config):
    username = get_database_username(database_config)
    if username:
        return username
    engine = get_database_engine(database_config)
    return get_database_default_username(engine)


def get_database_default_username(engine):
    return (DATABASE_USERNAME_MYSQL_DEFAULT
            if engine == DATABASE_ENGINE_MYSQL
            else DATABASE_USERNAME_POSTGRES_DEFAULT)


def get_database_password(database_config):
    return database_config.get(DATABASE_CONFIG_PASSWORD)


def get_database_password_with_default(database_config):
    password = get_database_password(database_config)
    if password:
        return password
    engine = get_database_engine(database_config)
    return get_database_default_password(engine)


def get_database_default_password(engine):
    return (DATABASE_PASSWORD_MYSQL_DEFAULT
            if engine == DATABASE_ENGINE_MYSQL
            else DATABASE_PASSWORD_POSTGRES_DEFAULT)


def get_database_name(database_config):
    return database_config.get(DATABASE_CONFIG_DATABASE)


def is_database_configured(database_config):
    if not database_config:
        return False
    return True if get_database_address(database_config) else False


def set_database_config(database_config, database_service):
    engine, service_addresses = database_service
    # take one address
    service_address = service_addresses[0]
    database_config[DATABASE_CONFIG_ENGINE] = get_validated_engine(engine)
    database_config[DATABASE_CONFIG_ADDRESS] = service_address[0]
    database_config[DATABASE_CONFIG_PORT] = service_address[1]


def with_database_environment_variables(database_config, envs=None):
    if not is_database_configured(database_config):
        return envs

    if envs is None:
        envs = {}

    envs[DATABASE_ENV_ENABLED] = True
    envs[DATABASE_ENV_ENGINE] = get_database_engine(database_config)
    envs[DATABASE_ENV_HOST] = get_database_address(database_config)
    envs[DATABASE_ENV_PORT] = get_database_port(database_config)

    # The defaults apply to built-in Database runtime.
    envs[DATABASE_ENV_USERNAME] = get_database_username_with_default(database_config)
    envs[DATABASE_ENV_PASSWORD] = get_database_password_with_default(database_config)

    return envs

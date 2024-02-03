import os

from cloudtik.core._private.util.core_utils import open_with_mode
from cloudtik.core._private.util.database_utils import get_database_username_with_default, \
    get_database_password_with_default, \
    get_database_address, get_database_port, get_database_name
from cloudtik.core._private.util.runtime_utils import get_runtime_config_from_node
from cloudtik.runtime.pgbouncer.utils import _get_config, _get_home_dir, _get_backend_databases, _get_backend_config, \
    _is_database_bind_user, _get_database_connect, _get_database_auth_user, \
    _get_database_auth_password


###################################
# Calls from node when configuring
###################################


def _get_config_dir():
    home_dir = _get_home_dir()
    return os.path.join(home_dir, "conf")


def _get_config_file():
    return os.path.join(_get_config_dir(), "pgbouncer.ini")


def _get_auth_file():
    return os.path.join(_get_config_dir(), "userlist.txt")


def configure_backend(head):
    runtime_config = get_runtime_config_from_node(head)
    pgbouncer_config = _get_config(runtime_config)
    backend_config = _get_backend_config(pgbouncer_config)
    # no matter static or dynamic, we need to the backend servers
    backend_databases = _get_backend_databases(backend_config)

    (username_password_map, username_password_conflicts) = _get_username_password_info(
        backend_databases)

    _update_auth_file(username_password_map)
    _update_backends(backend_databases, username_password_conflicts)


def _add_user_password(
        username_password_map, username_password_conflicts,
        username, password):
    if not username:
        return
    if username in username_password_conflicts:
        return
    if username not in username_password_map:
        username_password_map[username] = password
    else:
        existing_password = username_password_map[username]
        if existing_password != password:
            # conflicts
            username_password_map.pop(username)
            username_password_conflicts.add(username)


def _add_connect_username_password(
        username_password_map, username_password_conflicts,
        database_connect):
    if not database_connect:
        return
    username = get_database_username_with_default(database_connect)
    password = get_database_password_with_default(database_connect)
    _add_user_password(
        username_password_map, username_password_conflicts,
        username, password)


def _add_auth_username_password(
        username_password_map, username_password_conflicts,
        database_config):
    # WARNING: we should avoid auth user password conflicts
    username = _get_database_auth_user(database_config)
    password = _get_database_auth_password(database_config)
    _add_user_password(
        username_password_map, username_password_conflicts,
        username, password)


def _get_username_password_info(backend_databases):
    username_password_conflicts = set()
    username_password_map = {}
    for _, database_config in backend_databases.items():
        database_connect = _get_database_connect(database_config)
        _add_connect_username_password(
            username_password_map,  username_password_conflicts,
            database_connect)
        _add_auth_username_password(
            username_password_map, username_password_conflicts,
            database_config)
    return username_password_map, username_password_conflicts


def _escape_auth_value(auth_value):
    # replace each " with ""
    return auth_value.replace("\"", "\"\"")


def _update_auth_file(username_password_map):
    auth_file = _get_auth_file()
    # "username1" "password"
    # There should be at least 2 fields, surrounded by double quotes.
    # The first field is the username and the second is either a plain-text,
    # a MD5-hashed password, or a SCRAM secret.
    # PgBouncer ignores the rest of the line. Double quotes in a field value
    # can be escaped by writing two double quotes.
    user_password_lines = []
    # for same username but different passwords, we need put the password in the connect line
    for username, password in username_password_map.items():
        escaped_username = _escape_auth_value(username)
        escaped_password = _escape_auth_value(password)
        user_password_line = f"\"{escaped_username}\" \"{escaped_password}\""
        user_password_lines.append(user_password_line)
    user_password_block = "\n".join(user_password_lines)
    with open_with_mode(auth_file, "w", os_mode=0o600) as f:
        f.write(user_password_block)
        f.write("\n")


def _get_backend_database_lines(
        backend_databases, username_password_conflicts):
    backend_database_lines = []
    for database_name, database_config in backend_databases.items():
        backend_database_line = _get_backend_database_line(
            database_name, database_config, username_password_conflicts)
        if backend_database_line:
            backend_database_lines.append(backend_database_line)
    return backend_database_lines


def _get_backend_database_line(
        database_name, database_config, username_password_conflicts):
    database_connect = _get_database_connect(database_config)
    if not database_connect:
        return None
    host = get_database_address(database_connect)
    if not host:
        return None

    port = get_database_port(database_connect)
    connect_str = f"host={host} port={port}"

    db_name = get_database_name(database_connect)
    if db_name:
        connect_str += f" dbname={db_name}"

    if _is_database_bind_user(database_config):
        username = get_database_username_with_default(database_connect)
        connect_str += f" user={username}"
        if username in username_password_conflicts:
            # need use password in the connect line
            password = get_database_password_with_default(database_connect)
            connect_str += f" password={password}"

    auth_user = _get_database_auth_user(database_config)
    if auth_user:
        connect_str += f" auth_user={auth_user}"

    # Only available on 1.22.0 upwards
    """
    auth_query = _get_database_auth_query(database_config)
    if auth_query:
        # Simple query such as: SELECT usename, passwd FROM pg_shadow WHERE usename=$1
        connect_str += f" auth_query='{auth_query}'"
    """

    connect_line = "{} = {}".format(database_name, connect_str)
    return connect_line


def _update_backends(backend_databases, username_password_conflicts):
    # append the database connect string params at the end
    config_file = _get_config_file()
    backend_database_lines = _get_backend_database_lines(
        backend_databases, username_password_conflicts)
    backend_databases_block = "\n".join(backend_database_lines)
    with open(config_file, "a") as f:
        f.write(backend_databases_block)
        f.write("\n")

    return True

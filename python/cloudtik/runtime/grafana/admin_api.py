from cloudtik.core._private.util.rest_api import rest_api_get_json, rest_api_post_json, \
    rest_api_delete_json

REST_API_ENDPOINT_DATA_SOURCES = "/api/datasources"
REST_API_ENDPOINT_DATA_SOURCES_BY_NAME = REST_API_ENDPOINT_DATA_SOURCES + "/name"


def list_data_sources(admin_endpoint, auth):
    endpoint_url = "{}{}".format(
        admin_endpoint, REST_API_ENDPOINT_DATA_SOURCES)
    # The response is a list of data sources
    data_sources = rest_api_get_json(
        endpoint_url, auth=auth)
    return data_sources


def add_data_source(
        admin_endpoint, auth, data_source):
    endpoint_url = "{}{}".format(
        admin_endpoint, REST_API_ENDPOINT_DATA_SOURCES)
    # The response is response object with data_source in it
    # is there an exception when error?
    response_for_add = rest_api_post_json(
        endpoint_url, data_source, auth=auth)
    added_data_source = response_for_add.get("datasource")
    return added_data_source


def delete_data_source(admin_endpoint, auth, data_source_name):
    # DELETE /api/datasources/name/:datasourceName
    endpoint = "{}/{}".format(
        REST_API_ENDPOINT_DATA_SOURCES_BY_NAME, data_source_name)
    endpoint_url = "{}{}".format(
        admin_endpoint, endpoint)
    # is there an exception when error?
    response_for_delete = rest_api_delete_json(
        endpoint_url, auth=auth)
    return response_for_delete

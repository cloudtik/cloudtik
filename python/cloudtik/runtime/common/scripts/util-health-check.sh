#!/bin/bash

# The max post data length allowed
MAX_HTTP_POST_LENGTH=1024

# Functions

########################
# Read the HTTP headers from standard input, and parse and store their
# values in environment variables.
# Arguments:
#   The request uri
# Returns:
#   export HTTP_REQ_URI_PATH, HTTP_REQ_URI_PARAMS
#########################
http_parse_request_uri() {
    # example: /test123?r=123
    local -r request_uri="${1:-}"
    export HTTP_REQ_URI_PATH="$(echo "${request_uri}" | cut -d"?" -f 1)"
    if echo "$request_uri" | grep -q '?'; then
      export HTTP_REQ_URI_PARAMS="$(echo "${request_uri}" | cut -d"?" -f 2-)"
    else
      export HTTP_REQ_URI_PARAMS=""
    fi
}

########################
# Read the HTTP headers from standard input, and parse and store their
# values in environment variables.
# Arguments:
#   None
# Returns:
#   export HTTP_*
#########################
http_read_headers() {
    #
    # Read the HTTP headers from standard input, and parse and store their
    # values in environment variables.
    #
    while read -t 0.01 line; do
        line=${line//$'\r'}
        if [ ! -z "$VERBOSE" ]; then
          echo "H: $line"
        fi
        if [ -z "$line" ]; then break; fi
        if echo "${line}" | grep -qi "^GET\|POST\|PUT\|DELETE"; then
          # GET /test123?r=123 HTTP/1.1
          export HTTP_REQUEST="${line}"
          export HTTP_REQ_METHOD="$(echo "${line}" | cut -d" " -f 1)"
          export HTTP_REQ_URI="$(echo "${line}" | cut -d" " -f 2)"
          http_parse_request_uri "${HTTP_REQ_URI}"
          export HTTP_REQ_VERSION="$(echo "${line}" | cut -d" " -f 3-)"
        elif echo "${line}" | grep -qi "^User-Agent:"; then
          # User-Agent: curl/7.29.0
          export HTTP_USER_AGENT="$(echo "${line}" | cut -d" " -f 2-)"
        elif echo "${line}" | grep -qi "^Host:"; then
          # Host: 0.0.0.0:8081
          export HTTP_SERVER="$(echo "${line}" | cut -d" " -f 2-)"
        elif echo "${line}" | grep -qi "^Accept:"; then
          # Accept: */*
          export HTTP_ACCEPT="$(echo "${line}" | cut -d" " -f 2-)"
          #continue
        elif echo "${line}" | grep -qi "^Content-Length:"; then
          # Content-Length: 5
          export HTTP_CONTENT_LENGTH="$(echo "${line}" | cut -d" " -f 2-)"
        elif echo "${line}" | grep -qi "^Content-Type:"; then
          # Content-Type: application/x-www-form-urlencoded
          export HTTP_CONTENT_TYPE="$(echo "${line}" | cut -d" " -f 2-)"
        elif echo "${line}" | grep -qi "^X-Haproxy-Server-State:"; then
          # X-Haproxy-Server-State: UP; etc..
          export HTTP_HAPROXY_SERVER_STATE="$(echo "${line}" | cut -d" " -f 2-)"
        elif [ ${#line} -ge 1 ]; then
          # <any header>
          continue
        else
          break
        fi
    done
}

########################
# Read the HTTP POST data from standard input
# Arguments:
#   None
# Returns:
#   export HTTP_POST_CONTENT
#########################
http_read_data() {
    # This does not support a Content-type of multipart/mixed
    # This does not support chunking. It expects, and only allows, posted data to
    #   be the size of the Content-Length.
    #
    if [ "${HTTP_REQ_METHOD}" == "POST" ] && [ ${HTTP_CONTENT_LENGTH} -ge 1 ]; then
        export HTTP_POST_CONTENT=""
        DATA_LENGTH=$HTTP_CONTENT_LENGTH
        if [ ${DATA_LENGTH} -gt ${MAX_HTTP_POST_LENGTH} ]; then
          DATA_LENGTH=$MAX_HTTP_POST_LENGTH
        fi
        # If the value of Content-Length is greater than the actual content, then
        # read will timeout and never allow the collection from standard input.
        # This is overcome by reading one character at a time.
        #READ_BUFFER_LENGTH=1
        # If you are sure the value of Content-Length always equals the length of the
        # content, then all of standard input can be read in at one time
        READ_BUFFER_LENGTH=$DATA_LENGTH

        # Read POST data via standard input
        while IFS= read -N $READ_BUFFER_LENGTH -r -t 0.01 post_buffer; do
          let "DATA_LENGTH = DATA_LENGTH - READ_BUFFER_LENGTH"
          HTTP_POST_CONTENT="${HTTP_POST_CONTENT}${post_buffer}"
          # Stop reading if we reach the content length, max length, or expected length
          if [ ${#HTTP_POST_CONTENT} -ge ${HTTP_CONTENT_LENGTH} ]; then
            break;
          elif [ ${#HTTP_POST_CONTENT} -ge ${MAX_HTTP_POST_LENGTH} ]; then
            break;
          elif [ ${DATA_LENGTH} -le 0 ]; then
            break;
          fi
        done
        if [ ! -z "$VERBOSE" ]; then
          echo -e "D: $HTTP_POST_CONTENT"
        fi
    fi
}

########################
# Read the HTTP headers and data from standard input
# Arguments:
#   None
# Returns:
#   export HTTP_*
#########################
http_read() {
    http_read_headers
    http_read_data
}

########################
# A function to parse HTTP_REQ_URI_PARAMS and return the value of a given
# parameter name
# Arguments:
#   parameter name
# Returns:
#   The parameter value
#########################
http_get_request_param_value() {
    # Example: "a=123&b=456&c&d=789"
    local -r param_name=$1
    IFS='&' read -r -a params <<< "$HTTP_REQ_URI_PARAMS"
    for element in "${params[@]}"; do
      element_name="$(echo "$element" | cut -d"=" -f 1)"
      if [ "$element_name" == "$param_name" ]; then
        if echo "$element" | grep -q "="; then
          element_value="$(echo "$element" | cut -d"=" -f 2-)"
          echo "$element_value"
        else
          echo ""
        fi
        return 0
      fi
    done
    return 1
}

########################
# A function for HAProxy
# Parses the value in the HTTP header "X-Haproxy-Server-State"
# Arguments:
#   parameter name
# Returns:
#   The parameter value
# Examples:
#   HA_WEIGHT=$(get_haproxy_server_state_value weight) # HA_WEIGHT="1/2"
#   HA_STATE=$(get_haproxy_server_state_value state)   # UP, DOWN, NOLB
#########################
http_get_haproxy_server_state_value () {
    # Example: "UP; name=backend/server; node=haproxy-name; weight=1/2; scur=0/1; qcur=0; throttle=86%"
    local -r param_name=$1
    IFS='; ' read -r -a params <<< "$HTTP_HAPROXY_SERVER_STATE"
    if [ "$param_name" == "state" ]; then
      echo "${params[0]}"
      return 0
    fi
    for element in "${params[@]}"; do
      element_name="$(echo "$element" | cut -d"=" -f 1)"
      if [ "$element_name" == "$param_name" ]; then
        if echo "$element" | grep -q "="; then
          element_value="$(echo "$element" | cut -d"=" -f 2-)"
          echo "$element_value"
        else
          echo ""
        fi
        return 0
      fi
    done
    return 1
}

########################
# The HTTP response. This will return a HTTP response with the provided HTTP
# code and a descriptive message.
# Arguments:
#   http code
#   http message
# Returns:
#   None
# Example:
#   http_response 301 "You accessed something that does not exist"
#   http_response 200 '{ "status": "success" }'
#########################
http_response () {
    local -r http_code=$1
    local -r http_message=${2:-MESSAGE UNKNOWN}
    length=${#http_message}
    if [ "$http_code" -eq 503 ]; then
      echo -en "HTTP/1.1 503 Service Unavailable\r\n"
    elif [ "$http_code" -eq 301 ]; then
      echo -en "HTTP/1.1 301 Not Found\r\n"
    elif [ "$http_code" -eq 200 ]; then
      echo -en "HTTP/1.1 200 OK\r\n"
    else
      echo -en "HTTP/1.1 ${http_code} UNKNOWN\r\n"
    fi
    echo -en "Content-Type: text/plain\r\n"
    echo -en "Connection: close\r\n"
    echo -en "Content-Length: ${length}\r\n"
    echo -en "\r\n"
    echo -en "$http_message"
    echo -en "\r\n"
}

########################
# Response with a code and description message either by http or tcp
# based on whether there is HTTP_REQ_METHOD
# Arguments:
#   code
#   message
# Returns:
#   None
# Example:
#   response 301 "You accessed something that does not exist"
#   response 200 '{ "status": "success" }'
#########################
response () {
    local -r code=$1
    local -r message=${2:-Unknown}
    if [ ! -z "$HTTP_REQ_METHOD" ]; then
        http_response "$code" "$message"
    else
        echo "$message"
    fi
    exit 0
}

########################
# Show request information
# Arguments:
#   None
# Returns:
#   None
#########################
show_request_info() {
    REQUEST_INFO="$(env | grep '^HTTP')"
    if [ ! -z "$HTTP_POST_CONTENT" ]; then
      REQUEST_INFO="${REQUEST_INFO}\n--BEGIN:HTTP_POST_CONTENT--\n${HTTP_POST_CONTENT}\n--END:HTTP_POST_CONTENT--\n"
    fi
    if echo "$REQUEST_INFO" | grep -q '^HTTP'; then
      response 200 "$REQUEST_INFO"
    else
      response 200 "No request information."
    fi
}

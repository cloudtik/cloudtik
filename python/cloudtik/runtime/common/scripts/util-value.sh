#!/bin/bash
# Copyright VMware, Inc.
# SPDX-License-Identifier: APACHE-2.0
#
# shellcheck disable=SC1091,SC2086

# Functions

########################
# Check if the provided argument is an integer
# Arguments:
#   $1 - Value to check
# Returns:
#   Boolean
#########################
is_int() {
    local -r int="${1:?missing value}"
    if [[ "$int" =~ ^-?[0-9]+ ]]; then
        true
    else
        false
    fi
}

########################
# Check if the provided argument is a positive integer
# Arguments:
#   $1 - Value to check
# Returns:
#   Boolean
#########################
is_positive_int() {
    local -r int="${1:?missing value}"
    if is_int "$int" && (( "${int}" >= 0 )); then
        true
    else
        false
    fi
}

########################
# Check if the provided argument is a boolean or is the string 'yes/true'
# Arguments:
#   $1 - Value to check
# Returns:
#   Boolean
#########################
is_boolean_yes() {
    local -r bool="${1:-}"
    # comparison is performed without regard to the case of alphabetic characters
    shopt -s nocasematch
    if [[ "$bool" = 1 || "$bool" =~ ^(yes|true)$ ]]; then
        true
    else
        false
    fi
}

########################
# Check if the provided argument is a boolean yes/no value
# Arguments:
#   $1 - Value to check
# Returns:
#   Boolean
#########################
is_yes_no_value() {
    local -r bool="${1:-}"
    if [[ "$bool" =~ ^(yes|no)$ ]]; then
        true
    else
        false
    fi
}

########################
# Check if the provided argument is a boolean true/false value
# Arguments:
#   $1 - Value to check
# Returns:
#   Boolean
#########################
is_true_false_value() {
    local -r bool="${1:-}"
    if [[ "$bool" =~ ^(true|false)$ ]]; then
        true
    else
        false
    fi
}

########################
# Check if the provided argument is a boolean 1/0 value
# Arguments:
#   $1 - Value to check
# Returns:
#   Boolean
#########################
is_1_0_value() {
    local -r bool="${1:-}"
    if [[ "$bool" =~ ^[10]$ ]]; then
        true
    else
        false
    fi
}

########################
# Check if the provided argument is an empty string or not defined
# Arguments:
#   $1 - Value to check
# Returns:
#   Boolean
#########################
is_empty_value() {
    local -r val="${1:-}"
    if [[ -z "$val" ]]; then
        true
    else
        false
    fi
}

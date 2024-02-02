#!/bin/bash

install_postgres_repository() {
    # download the signing key
    # wget -O - -q https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
    # echo "deb http://apt.postgresql.org/pub/repos/apt/ `lsb_release -cs`-pgdg main" \
    #     | sudo tee /etc/apt/sources.list.d/postgres.list

    key='B97B0AFCAA1A47F044F244A07FCC7D46ACCC4CF8' \
      && export GNUPGHOME="$(mktemp -d)" \
      && sudo gpg --batch --keyserver keyserver.ubuntu.com --recv-keys "$key" >/dev/null 2>&1 \
      && sudo mkdir -p /usr/local/share/keyrings/ \
      && sudo gpg --batch --export --armor "$key" | sudo tee /usr/local/share/keyrings/postgres.gpg.asc >/dev/null \
      && sudo gpgconf --kill all \
      && rm -rf "$GNUPGHOME" \
      && echo "deb [ signed-by=/usr/local/share/keyrings/postgres.gpg.asc ] http://apt.postgresql.org/pub/repos/apt/ `lsb_release -cs`-pgdg main" \
        | sudo tee /etc/apt/sources.list.d/postgres.list >/dev/null
}

uninstall_postgres_repository() {
    sudo rm -f /etc/apt/sources.list.d/postgres.list
}

#!/usr/bin/env bash

# This works on ubuntu with a sudo capable user

# Update the apt package index and install packages to allow apt to use a repository over HTTPS
sudo apt-get update -y
sudo apt-get install -y \
    ca-certificates \
    curl \
    gnupg \
    lsb-release

# Add Docker's official GPG key:
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --yes --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg

#  Set up the stable repository
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

# Update the apt package index, and install the latest version of Docker Engine and containerd
sudo apt-get update -y
sudo apt-get install -y docker-ce docker-ce-cli containerd.io

if [[ "$(id -u)" != "0" ]]; then
    getent group "docker" >/dev/null 2>&1
    if [ $? != 0 ]; then
        sudo groupadd docker
    fi
    current_user=$(whoami)
    sudo gpasswd -a $current_user docker
fi

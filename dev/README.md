# Developer Guide
## Setup the development environment
If you have a fresh Ubuntu machine, you can execute the following
command to setup the development environment including installing the
dev tools, conda, docker; and building CloudTik wheel and install it
to a conda env:

```
bash ./dev/setup-all.sh
```

## Develop and test clusters locally with virtual provider
The virtual provider is a great option to use in development. You can test
many runtime features without going to a real public cloud provider and
creating real virtual machines there.

Within your development machine, the virtual provider use docker and containers
which makes it easy to simulate and test many clusters with many nodes on a single node.

Here is a typical cluster configuration using virtual provider for testing:

```
# A unique identifier for the cluster.
cluster_name: example

# The workspace name
workspace_name: example-workspace

cloudtik_wheel_url: file:///cloudtik/data/share/cloudtik-1.4.0-cp38-cp38-manylinux2014_x86_64.nightly.whl

# Cloud-provider specific configuration.
provider:
    type: virtual

auth:
    ssh_user: ubuntu

available_node_types:
    head.default:
        node_config:
            data_disks:
                - /tmp/cloudtik
            data_dirs:
                - /home/ubuntu/share
    worker.default:
        node_config:
            data_disks:
                - /tmp/cloudtik
            data_dirs:
                - /home/ubuntu/share
        min_workers: 3
```

Every time after recompiled the dev version of the wheel,
copy the new wheel to the data sharing folder "/home/ubuntu/share".

## Release procedure
If the version has been bumped up and all source code in main are ready to release,
execute the following procedure to finish a release.

### Step 1: Create the branch and tag for the release version
This step will create the branch and tag based on main. Execute:
```
bash ./dev/release-branch.sh
```
The version information is retrieved from python code "__version__" variable.
Username and the token for Github is needed when prompt. 

### Step 2: Build the wheels and release to AWS S3 bucket
This step will build the wheels for different python versions
and upload the wheels to AWS S3 as the backup download location. 
Execute:
```
bash ./dev/release.sh --branch branch-<version>
```

### Step 3: Release docker images (if necessary)
This step will build all the docker images and upload to docker hub
of cloudtik account.
Execute:
```
bash ./dev/release-docker.sh --release-all --image-tag <version>
```
For build images and push to registry for global and China:
```
bash ./dev/release-docker.sh --release-all --region PRC --image-tag <version>
```
For build GPU images and push to registry for global and China:
```
bash ./dev/release-docker.sh --release-all --region PRC --gpu --image-tag <version> 
```

### Step 4: Release wheels to PyPI
This step will upload all the wheels in the dist folder under python folder
to PyPI with cloudtik account.
Execute:
```
bash ./dev/release-pip.sh
```
When prompt, input the username and password.

### Step 5: Create a release at Github
Finally, we create a release for Github CloudTik repository.

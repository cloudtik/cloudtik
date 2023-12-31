# Running On-Premise Clusters
CloudTik can easily manage and scale the resources on public cloud through the cloud API/SDK.
While users sometimes want to do some performance tests on their local machines. 
In order to manage local machine resources conveniently, CloudTik has developed a Cloud Simulator service 
that runs on on-premise/private clusters to simulate cloud operations and create one or more clusters on a local machine pool.
With the cloud-simulator, CloudTik implements an on-premise provider which calls into Cloud Simulator
to create and release nodes from the machine pool.

Please follow these steps to use for On-Premise clusters.

- System requirements
- Prepare the machines
- Configure and start CloudTik Cloud Simulator
- Configure and start the cluster

## System requirements
The participating nodes need Ubuntu 20.04 or later.

## Prepare the machines
For the machines used for CloudTik, there are a few requirements. 
1. All the machines needs to have a user with sudo privilege. A non-root user is suggested, for example 'cloudtik'.
2. Prepare the private key login for the nodes
3. Setup host resolution for all the nodes
4. Prepare the local disks

For public cloud providers, the virtual machines are created with #1, #2 and #3 already satisfied.
For #4 (disks) on the public cloud, CloudTik will list all raw block devices,
create a file system for them and mount to /mnt/cloudtik/data_disk_#.
CloudTik runtime will automatically search disks under /mnt/cloudtik.
And use these disks as data disks.

For on-premise clusters, user need to make sure these requirements are satisfied manually or through
utility scripts.

### Create a new sudo user
Cloudtik doesn't suggest to use root user to manage the cluster, 
so you need to create a normal user with sudo privileges for each of your machines.
If such a user already exists, you can skip this step.
For example to create cloudtik user:
```buildoutcfg
sudo useradd -ms /bin/bash -d /home/cloudtik cloudtik;
sudo usermod -aG sudo cloudtik;
echo 'cloudtik ALL=NOPASSWD: ALL' | sudo tee -a /etc/sudoer;
sudo passwd cloudtik;
```

### Prepare the private key file login for nodes
We need to configure the private key file login for each participating nodes.
Use the command `ssh-keygen` to generate a new ssh key pair on working node.
For example:

`ssh-keygen -t rsa -b 4096`

Then add the generated ssh public key to each node for enabling its private key file login.
One simple way to do this is to use `ssh-copy-id` command. For example:

```
ssh-copy-id -i ~/.ssh/id_rsa.pub [sudo user]@[node-ip]
```

### Set up host resolution
We need to make sure that the host resolution is configured and working properly. 
This resolution can be done by using a DNS server or by configuring the "/etc/hosts" file on each node we use for cluster setting up. 
Then you also need to generate a new ssh key pair on working node and add this SSH public key to each nodes. 
Cloudtik will use this private key to login in the cluster.


### Prepare local disks
Cloudtik will automatically detect raw block devices on each node, format the disks without partitions
and mount these disks in the directory "/mnt/cloudtik/data_disk_[number]" specified by Cloudtik.

So you can either leave the data disks as raw block devices and CloudTik will do all for you
in the same way as we do for public cloud virtual machines.

Or if the data disks have already been formatted and mounted at other paths,
you can create a symbolic link from your paths to /mnt/cloudtik/data_disk_#.
By utilizing custom initialization command provided by CloudTik, you can easily to do these
in your cluster without manually doing this. For example,

```
initialization_commands:
    - sudo ln -s /mnt/your/disk_1 /mnt/cloudtik/data_disk_1
    - sudo ln -s /mnt/your/disk_2 /mnt/cloudtik/data_disk_2
```

Note: please sure your data disk paths set the right permission for read/write by all users.
If not, you can add the corresponding commands to initialization command as well.

## Configure and start CloudTik Cloud Simulator
You need prepare the CloudTik Cloud Simulator configure file and
start CloudTik Cloud Simulator service with the configure file.

### Create and configure a YAML file for cloudtik-cloud-simulator 
You need to provide your machine hardware configuration and ip.
```buildoutcfg
# Define one or more instance types with the information of its hardware resources
# Then you specify the instance type for each node in the node list
instance_types:
    head_instance_type:
        # Specify the resources of this instance type.
        resources:
            CPU: number-of-cores
            memoryMb: size-in-mb
    worker_instance_type_1:
        # Specify the resources of this instance type.
        resources:
            CPU: number-of-cores
            memoryMb: size-in-mb
    worker_instance_type_2:
        # Specify the resources of this instance type.
        resources:
            CPU: number-of-cores
            memoryMb: size-in-mb

# List of nodes with the ip and its node type defined in the above list
nodes:
    - ip: node_1_ip
      # Should be one of the instance types defined in instance_types
      instance_type: head_instance_type
      # You may need to supply a public ip for the head node if you need
      # to start cluster from outside of the cluster's network
      # external_ip: your_head_public_ip
    - ip: node_2_ip
      instance_type: worker_instance_type_1
    - ip: node_3_ip
      instance_type: worker_instance_type_1
    - ip: node_4_ip
      instance_type: worker_instance_type_2

```

### Start cloudtik-cloud-simulator service
```buildoutcfg
cloudtik-simulator [--bind-address BIND_ADDRESS] [--port PORT] your_cloudtik_simulator_config
```

## Create workspace
With on-premise provider, you can run clusters in different conceptual workspace.

In the workspace configuration, you need to specify the cloud_simulator_address.

For example,

```buildoutcfg
# A unique identifier for the workspace.
workspace_name: example-workspace

# Cloud-provider specific configuration.
provider:
    type: onpremise
    # We need to use Cloud Simulator for the best on-premise cluster management
    # You can launch multiple clusters on the same set of machines, and the cloud simulator
    # will assign individual nodes to clusters as needed.
    cloud_simulator_address: your-cloud-simulator-ip:port
```

Execute the following command to create the workspace:
```
cloudtik workspace create /path/to/your-workspace-config.yaml
```


## Configure and start cluster
You need prepare the cluster configure file using on-premise provider and
start the cluster with the cluster configure file.

### Create and configure a YAML file for cluster

Define cloud_simulator_address for on-premise provider. (Default port is 8282)
```buildoutcfg
# A unique identifier for the cluster.
cluster_name: example

# The workspace name
workspace_name: example-workspace

# Cloud-provider specific configuration.
provider:
    type: onpremise

    # We need to use Cloud Simulator for the best on-premise cluster management
    # You can launch multiple clusters on the same set of machines, and the cloud simulator
    # will assign individual nodes to clusters as needed.
    cloud_simulator_address: your-cloud-simulator-ip:port
```

Define ssh user and its ssh private key which are prepared above.
You also need to provide ssh_proxy_command if the head node needs to access the worker node through proxy.
```buildoutcfg
auth:
    # The ssh user configured above with sudo privilege
    ssh_user: cloudtik
    # Specify the private key file for login to the nodes
    ssh_private_key: ~/.ssh/id_rsa
    # Set proxy if you are in corporation network. For example,
    # ssh_proxy_command: "ncat --proxy-type socks5 --proxy your_proxy_host:your_proxy_port %h %p"

```
Define available_node_types for head and worker. 
```buildoutcfg
available_node_types:
    head.default:
        node_config:
            # The instance type used here need to be defined in the instance_types
            # in the Cloud Simulator configuration file
            instance_type: head_instance_type
    worker.default:
        min_workers: 2
        node_config:
            instance_type: worker_instance_type_1
```

### Start the cluster with the configure file
Starting the cluster is simple,
```buildoutcfg
cloudtik start /path/to/your-cluster-config.yaml
```

# Quick Start

### 1. Preparing Python environment

CloudTik requires a Python environment on Linux. We recommend using Conda to manage Python environments and packages.

If you don't have Conda installed, please refer to `dev/install-conda.sh` to install Conda on Linux.

```
git clone https://github.com/cloudtik/cloudtik.git && cd cloudtik
bash dev/install-conda.sh
```

Once Conda is installed, create an environment with a specific Python version as below.
CloudTik currently supports Python 3.8 or above. Take Python 3.9 as an example,

```
conda create -n cloudtik -y python=3.9
conda activate cloudtik
```

### 2. Installing CloudTik

Execute the following `pip` commands to install CloudTik on your working machine for specific cloud providers. 

Take AWS for example,

```
pip install cloudtik[aws]
```

Replace `cloudtik[aws]` with `clouditk[azure]`, `cloudtik[gcp]`, `cloudtik[aliyun]`
if you want to create clusters on Azure, GCP, Alibaba Cloud respectively.

If you want to run on Kubernetes, install `cloudtik[kubernetes]`.
Or  `clouditk[eks]` or `cloudtik[gke]` if you are running on AWS EKS or GCP GKE cluster.
Use `cloudtik[all]` if you want to manage clusters with all supported Cloud providers.

Please refer to [User Guide: Installation](../UserGuide/installation.md) for the package links for other Python versions.

If you don't have a public cloud account, you can also play with CloudTik
easily locally with the same clustering experiences using virtual, on-premise or local providers.
For this case, simply install cloudtik core as following command,
```
pip install cloudtik
```
Please refer to [User Guide: Running Clusters Locally](../UserGuide/running-locally.html)
for detailed guide for this case.

### 3. Authentication to Cloud Providers API
After CloudTik is installed on your working machine, you need to configure or log into your Cloud account to 
authenticate the cloud provider CLI on the working machine.

Take AWS for example, install AWS CLI (command line interface),
run `aws configure` command and input your *AWS Access Key ID* and *AWS Secret Access Key*.
CloudTik is able to pick up your client credentials you configured through `aws configure`.

For detailed information of how to authenticate with public cloud providers,
refer to [User Guide: Login to Cloud](../UserGuide/login-to-cloud.html)

### 4. Creating a Workspace for Clusters.
Once you authenticated with your cloud provider, you can start to create a Workspace.

CloudTik uses **Workspace** concept to easily manage shared Cloud resources such as VPC network resources,
identity and role resources, firewall or security groups, and cloud storage resources.
By default, CloudTik will create a workspace managed cloud storage
(S3 for AWS, Data Lake Storage Gen 2 for Azure, GCS for GCP) for use without any user configurations.

**Note: Some resources like NAT gateway or elastic IP resources in Workspace cost money.
The price policy may vary among cloud providers.
Please check the price policy of the specific cloud provider to avoid undesired cost.**

Within a workspace, you can start one or more clusters with different combination of runtime services.

Create a configuration workspace yaml file to specify the unique workspace name, cloud provider type and a few cloud 
provider properties. 

Take AWS as an example,

```
# A unique identifier for the workspace.
workspace_name: example-workspace

# Cloud-provider specific configuration.
provider:
    type: aws
    region: us-west-2
    # Use allowed_ssh_sources to allow SSH access from your client machine
    allowed_ssh_sources:
      - 0.0.0.0/0
```
*NOTE:* `0.0.0.0/0` in `allowed_ssh_sources` will allow any IP addresses to connect to your cluster as long as it has the cluster private key.
For more security, you need to change from `0.0.0.0/0` to restricted CIDR ranges for your case.

Use the following command to create and provision a Workspace:

```
cloudtik workspace create /path/to/your-workspace-config.yaml
```

Check [Configuration Examples](https://github.com/cloudtik/cloudtik/tree/main/examples/cluster) folder for more Workspace configuration file examples
for AWS, Azure, GCP, Kubernetes (AWS EKS or GCP GKE).

If you encounter problems on creating a Workspace, a common cause is that your current login account
for the cloud doesn't have enough privileges to create some resources such as VPC, storages, public ip and so on.
Make sure your current account have enough privileges. An admin or owner role will give the latest chance to have
all these privileges.

### 5. Starting a cluster with default runtimes

Now you can start a cluster with a combination of runtimes you want.
By default, it will include Spark runtime.

```
cloudtik start /path/to/your-cluster-config.yaml
```

A typical cluster configuration file is usually very simple thanks to design of CloudTik's templates with inheritance.

Take AWS for example,

```
# An example of standard 1 + 3 nodes cluster with standard instance type
from: aws/standard

# Workspace into which to launch the cluster
workspace_name: example-workspace

# A unique identifier for the cluster.
cluster_name: example

# Cloud-provider specific configuration.
provider:
    type: aws
    region: us-west-2

auth:
    ssh_user: ubuntu
    # Set proxy if you are in corporation network. For example,
    # ssh_proxy_command: "ncat --proxy-type socks5 --proxy your_proxy_host:your_proxy_port %h %p"

available_node_types:
    worker.default:
        # The minimum number of worker nodes to launch.
        min_workers: 3
```

This example can be found in CloudTik source code folder `examples/cluster/aws/example-standard.yaml`.

It will start a cluster named "example" in workspace "example-workspace" with minimal of 3 worker nodes running
Spark runtime services by default.

You need only a few key settings in the configuration file to launch a Spark cluster.

As for `auth` above, please set proxy if your working node is using corporation network.

```
auth:
    ssh_user: ubuntu
    ssh_proxy_command: "ncat --proxy-type socks5 --proxy <your_proxy_host>:<your_proxy_port> %h %p"
```

The cluster key will be created automatically for AWS and GCP if not specified.
For Azure, you need to generate an RSA key pair manually (use `ssh-keygen -t rsa -b 4096` to generate a new ssh key pair).
and configure the public and private key as following,

```
auth:
    ssh_private_key: ~/.ssh/my_cluster_rsa_key
    ssh_public_key: ~/.ssh/my_cluster_rsa_key.pub
```
If you need different runtime components in the cluster,
in the cluster configuration file, you can set the runtime types. For example,
```
runtime:
    types: [hdfs, spark, ai]
```
It will run a cluster with hdfs, spark and ai runtimes.

Refer to `examples/cluster` directory for more cluster configurations examples.

### 6. Running analytics and AI workloads

Once the cluster is started, you can run Spark analytics and AI workloads
which are designed to be distributed and large scale in nature.

Below provides the information of some basic examples to start with.
As to running optimized Spark and AI, you can refer to [Running Optimized Analytics with Spark](https://cloudtik.readthedocs.io/en/latest/UserGuide/running-optimized-ai.html)
and [Running Optimized AI](https://cloudtik.readthedocs.io/en/latest/UserGuide/running-optimized-ai.html) for more information.

#### Running spark PI example

Running a Spark job is very straight forward. Spark PI job for example,

```
cloudtik exec ./your-cluster-config.yaml "spark-submit --master yarn --deploy-mode cluster --name spark-pi --class org.apache.spark.examples.SparkPi --conf spark.yarn.submit.waitAppCompletion=false \$SPARK_HOME/examples/jars/spark-examples.jar 12345" --job-waiter=yarn
```

Refer to [Run Spark PI Example](https://github.com/cloudtik/cloudtik/tree/main/examples/runtime/spark) for more details.

#### Running analytics benchmarks

CloudTik provides ready to use tools for running TPC-DS benchmark
on a CloudTik spark runtime cluster.

Refer to [Run TPC-DS performance benchmark for Spark](https://github.com/cloudtik/cloudtik/tree/main/tools/benchmarks/spark)
for a detailed step-by-step guide.

#### Running machine learning and deep learning examples

CloudTik provides ready to run examples for demonstrating
how distributed AI jobs can be implemented in CloudTik Spark and AI runtime cluster.

Refer to [Distributed AI Examples](https://github.com/cloudtik/cloudtik/tree/main/examples/runtime/ai)
for a detailed step-by-step guide.

#### Workflow examples
User can integrate CloudTik with external workflows using bash scripts or python
for running on-demand cluster and jobs.

Refer to [Workflow Integration Examples](https://github.com/cloudtik/cloudtik/tree/main/examples/workflows) for example scripts.

### 7. Managing clusters

CloudTik provides very powerful capability to monitor and manage the cluster.

#### Cluster status and information

Use the following commands to show various cluster information.

```
# Check cluster status with:
cloudtik status /path/to/your-cluster-config.yaml
# Show cluster summary information and useful links to connect to cluster web UI.
cloudtik info /path/to/your-cluster-config.yaml
cloudtik head-ip /path/to/your-cluster-config.yaml
cloudtik worker-ips /path/to/your-cluster-config.yaml
```
#### Attach to the cluster head (or specific node)

Connect to a terminal of cluster head node.

```
cloudtik attach /path/to/your-cluster-config.yaml
```

#### Execute and Submit Jobs

Execute a command via SSH on cluster head node or a specified node.

```
cloudtik exec /path/to/your-cluster-config.yaml [command]
```

#### Manage Files

Upload files or directories to cluster.

``` 
cloudtik upload /path/to/your-cluster-config.yaml [source] [target]
```
  
Download files or directories from cluster.

```
cloudtik download /path/to/your-cluster-config.yaml [source] [target]
```

### 8. Tearing Down

#### Terminate a Cluster

Stop and delete the cluster.

```
cloudtik stop /path/to/your-cluster-config.yaml
```

#### Delete a Workspace

Delete the workspace and all the network resources within it.

```
cloudtik workspace delete /path/to/your-workspace-config.yaml
```
Be default, the managed cloud storage will not be deleted.
Add --delete-managed-storage option to force deletion of manged cloud storage.

For more information as to the commands, you can use `cloudtik --help` or `cloudtik [command] --help` to get detailed instructions.

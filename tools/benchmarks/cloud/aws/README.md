# Run TPC-DS power test with SF1000 for Spark on AWS

Here we provide a guide for you to set up a cluster with 1 m5.4xlarge instance as head and three m5.16xlarge instances as workers on AWS and use S3 to store data.
You can easily generate 1 TB data for TPC-DS and run TPC-DS power test with Spark.

This can be done simply with CloudTik commands. We provide two wrapper scripts **[aws-cloudtik.sh](./scripts/aws-cloudtik.sh)** and **[aws-benchmark.sh](./scripts/aws-benchmark.sh)**
for helping make the work even simpler. Fundamentally, the wrapper scripts use cloudtik command to perform the real work under-layer.

We didn't package these helper scripts in CloudTik pip package.
So we assume you have cloned the CloudTik source to your local machine.
And export CLOUDTIK_HOME environment variable with value pointing to the CloudTik source code root for convenience.

```buildoutcfg
# Clone CloudTik repository
git clone https://github.com/cloudtik/cloudtik.git

# Export CLOUDTIK_HOME
export CLOUDTIK_HOME=$PWD/cloudtik
```

## 1. Create and configure a YAML file for workspace
We provide an example workspace yaml file **[aws-workspace.yaml](./aws-workspace.yaml)**, 
and you can modify this example file according to your requirements. 

## 2. Create a workspace on AWS
If you haven't yet installed CloudTik or configured AWS login credentials,
Please refer to [CloudTik Installation](https://cloudtik.readthedocs.io/en/latest/UserGuide/installation.html)
and [Login to Cloud](https://cloudtik.readthedocs.io/en/latest/UserGuide/login-to-cloud.html) for details.
After installed CloudTik and configured AWS login credentials,
use the wrapper script **[aws-cloudtik.sh](./scripts/aws-cloudtik.sh)** to create a workspace.

```buildoutcfg
# Create a workspace on AWS
bash $CLOUDTIK_HOME/tools/benchmarks/cloud/aws/scripts/aws-cloudtik.sh --action create-workspace --config $CLOUDTIK_HOME/tools/benchmarks/cloud/aws/aws-workspace.yaml --yes
```

## 3. Create and configure a YAML file for cluster
We provide an example cluster yaml file **[aws-large-cluster-with-s3.yaml](./aws-large-cluster-with-s3.yaml)**, 
and you need to modify this yaml file according to your workspace configuration. 

## 4. Create a cluster on AWS
Use the wrapper script **[aws-cloudtik.sh](./scripts/aws-cloudtik.sh)** to start the cluster. 

```buildoutcfg
# Start a cluster on AWS
bash $CLOUDTIK_HOME/tools/benchmarks/cloud/aws/scripts/aws-cloudtik.sh --action start-cluster --config $CLOUDTIK_HOME/tools/benchmarks/cloud/aws/aws-large-cluster-with-s3.yaml --yes
```

## 5. Generate data
Use the wrapper script **[aws-benchmark.sh](./scripts/aws-benchmark.sh)** to generate data for TPC-DS in different scales.
```buildoutcfg
# Generate TPC-DS SF1000 data
bash $CLOUDTIK_HOME/tools/benchmarks/cloud/aws/scripts/aws-benchmark.sh --action generate-data --cluster_config $CLOUDTIK_HOME/tools/benchmarks/cloud/aws/aws-large-cluster-with-s3.yaml --workspace_config $CLOUDTIK_HOME/tools/benchmarks/cloud/aws/aws-workspace.yaml --scale_factor 1000
```

## 6. Run TPC-DS power test with vanilla Spark
Use the wrapper script **[aws-benchmark.sh](./scripts/aws-benchmark.sh)** for you to easily run TPC-DS power test with vanilla spark.
```buildoutcfg
# Run TPC-DS power test with SF1000 for 1 round 
bash $CLOUDTIK_HOME/tools/benchmarks/cloud/aws/scripts/aws-benchmark.sh --action power-test --cluster_config $CLOUDTIK_HOME/tools/benchmarks/cloud/aws/aws-large-cluster-with-s3.yaml --workspace_config $CLOUDTIK_HOME/tools/benchmarks/cloud/aws/aws-workspace.yaml --scale_factor 1000 --iteration=1 --baseline
```

## 7 . Clean up AWS cluster and workspace
After you have done all the work and no longer need the cluster or workspace.
use the wrapper script **[aws-cloudtik.sh](./scripts/aws-cloudtik.sh)** to stop the cluster
and optionally delete the workspace.

```buildoutcfg
# Stop the cluster
# bash $CLOUDTIK_HOME/tools/benchmarks/cloud/aws/scripts/aws-cloudtik.sh --action stop-cluster --config $CLOUDTIK_HOME/tools/benchmarks/cloud/aws/aws-large-cluster-with-s3.yaml --yes

# Optionally delete the workspace
# bash $CLOUDTIK_HOME/tools/benchmarks/cloud/aws/scripts/aws-cloudtik.sh --action delete-workspace --config $CLOUDTIK_HOME/tools/benchmarks/cloud/aws/aws-workspace.yaml --yes
```

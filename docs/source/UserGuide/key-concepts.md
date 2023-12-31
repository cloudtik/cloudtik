# Key Concepts

This section gives an introduction to CloudTik’s key concepts.

- [Workspace](#workspace)
- [Cluster](#cluster)
- [Provider](#provider)
- [Runtime](#runtime)

## Workspace

CloudTik uses **Workspace** concept to manage shared Cloud specific resources which needed for running a production level
platform. These resources include
- VPC and subnets
- firewall or security groups
- gateways
- identities and roles
- cloud object storage and databases

These resources are usually created once and shared among different clusters. CloudTik provides the commands to help user
create or delete a workspace on a specific Cloud, which will provision and configured the right resources to be ready to use.

**Note: Some resources like NAT gateway or elastic IP resources in Workspace cost money.
The price policy may vary among cloud providers.
Please check the price policy of the specific cloud provider to avoid undesired cost.**

User can create one or more workspaces on a Cloud provider as long as the resources limits are not reached.
Since some resources in Workspace cost money on time basis,
to save your cost, don't create unnecessary workspaces.

Within each workspace, user can start one or more clusters with necessary runtime components provisioned and configured in use.


## Cluster

Cluster is a scalable collection of nodes with necessary runtime services running and provides analytics and AI services
to the user. A Cluster is a self-managed organism which is not only include the hardware infrastructure but also the services
that running over it and works together to form a self-recoverable and servicing organism.

A CloudTik cluster consists of one head node and zero or more worker nodes. The head node is the brain of the cluster
and runs services for creating, setting up and starting new worker nodes, recovering an unhealthy worker node and so on.
Cloudtik provides powerful and easy to use facilities for users to quickly create and manage analytics and AI clusters.

A cluster belongs to a Workspace and shares the common workspace resources including VPC, subnets, gateways and so on.
All clusters within the same workspace are network connected and are accessible by each other.

## Provider
CloudTik provider abstracts the hardware infrastructure layer so that CloudTik common facilities and runtimes
can consistently run on every provider environments. The support of different public cloud are implemented as providers
(such as AWS, Azure, GCP providers). Beyond the public cloud environments, we also support
virtual single node clustering, local or on-premise clusters which are also implemented as providers
(for example, virtual, local and on-premise providers)
We currently support many public cloud providers and several special providers for on-premise clusters.
More provider implementations will be added soon.
- AWS
- Azure
- GCP
- Alibaba Cloud
- Kubernetes (or EKS, AKS and GKE)
- Virtual: virtual clustering on single node
- Local: single local clustering with multiple nodes
- On-premise: cloud simulating on on-premise nodes

In the design level, provider is a concept to abstract out the difference of public providers. With this abstraction, CloudTik is designed to share
the same user experiences among different cloud providers as much as possible. Internally, we have two aspects of abstraction
for a specific provider:
- Node Provider: Abstraction of how a cloud provider to provide services of creating or terminating instances and instances managing functionalities.
- Workspace Provider: Abstraction of how a cloud provider to implement the CloudTik workspace design on network and securities.

## Runtime

Cloudtik introduces **Runtime** concept to abstract a distributed service or a collection of related services running
on head and worker nodes. Different runtimes share some high level but common patterns like installing, configuring,
starting, stopping and so on. CloudTik provides infrastructure for orchestration of these aspects and allow a runtime
to handle its very specific implementation. In this way, a runtime can easy be implemented and orchestrated into
CloudTik and works together with other runtimes by providing services to other runtimes or consuming other runtime services.

CloudTik runtimes are functional components to provide virtually some services.
Although the runtimes are decoupled and can be selected to include in a cluster independently,
CloudTik runtimes are designed to connect and consume other runtime services in the same workspace
through various service discovery mechanisms.

For example, if you configure a cluster to run HDFS, MySQL, Metastore and Spark runtimes,
no need for additional configuration, Metastore will discover MySQL service
and will use it as Metastore database; Spark will discover HDFS and Metastore service
and will use HDFS as Spark storage and Metastore as Spark catalog store.
The same will work smartly even if the runtimes are in different clusters
as long as they are in the same workspace.

A more comprehensive example is running with microservices architecture.
A cluster configured with consul on server mode started as the first cluster in the workspace,
which provide service registry, service discovery and service DNS capabilities to the workspace.
One cluster started with other runtimes, for example, Metastore and Spark runtimes.
When there is service discovery runtime in the workspace, the workspace runtimes will automatically
register the service of the runtime and will discover any potential runtime services it consumes.
Another cluster started with HAProxy runtime with a simple configuration of a service
name to load balancing for. HAProxy runtime which will automatically discover all the
service instances that are running and be configured to do load balancing for the configured service.

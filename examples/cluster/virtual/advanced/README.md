# An advanced example
This advanced example demonstrate many fundamental features used in
a typical microservices architecture environments.

## Use the example
The default configurations will make sure the services of different clusters
are orchestrating through service discovery, naming services, and load balancer.

### Create the workspace
```
cloudtik workspace create ./example-workspace.yaml
```

### Start bootstrap cluster
```
cloudtik start ./example-bootstrap.yaml
```

### Start MinIO storage cluster
```
cloudtik start ./example-minio.yaml
```

### Start Spark cluster
```
cloudtik start ./example-spark.yaml
```

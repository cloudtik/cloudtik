# Running Optimized Spark

Using the default configurations, you are using the upstream Spark version
which is not optimized by CloudTik.

Just as we mentioned in [Spark Optimizations](spark-optimizations.md),
CloudTik implements many Spark optimizations upon the upstream Spark and
you can achieve better performance by turning on these optimizations.

## Running Spark with optimizations
Running Spark with the optimizations in [Spark Optimizations](spark-optimizations.md) is available
when you are using container mode (this is default).

To turn on these optimizations, add the following configuration to the docker section
of your cluster configuration file:

```

docker:
    image: "cloudtik/spark-optimized"

```

This will use the docker image which is built with optimized Spark version.

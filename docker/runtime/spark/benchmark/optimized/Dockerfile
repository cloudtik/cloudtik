ARG BASE_TAG="nightly"
FROM cloudtik/spark-optimized:$BASE_TAG

# Compile spark-sql-perf, tpcds-kit, tpch-dbgen, HiBench
RUN wget https://raw.githubusercontent.com/cloudtik/cloudtik/main/tools/benchmarks/spark/scripts/bootstrap-benchmark.sh -O /tmp/bootstrap-benchmark.sh \
    && bash /tmp/bootstrap-benchmark.sh --workload=all \
    && rm -rf /tmp/bootstrap-benchmark.sh

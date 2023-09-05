ARG BASE_PREFIX=""
ARG BASE_TAG="nightly"
FROM cloudtik/${BASE_PREFIX}ai-runtime:$BASE_TAG

# Deploy TPCx-AI toolkit
RUN wget https://raw.githubusercontent.com/cloudtik/cloudtik/main/tools/benchmarks/ai/tpcx-ai/scripts/bootstrap-tpcx-ai.sh -O /tmp/bootstrap-tpcx-ai.sh \
    && bash /tmp/bootstrap-tpcx-ai.sh --head \
    && rm -rf /tmp/bootstrap-tpcx-ai.sh

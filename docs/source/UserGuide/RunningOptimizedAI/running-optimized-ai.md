# Running Optimized AI

By using cloudtik/spark-ai-runtime image,
you get the basic machine learning and deep learning performance
through the open source frameworks like TensorFlow or PyTorch.

```
docker:
    image: "cloudtik/spark-ai-runtime"
```

To gain better performance on CPU and make full use of modern hardware features,
the machine learning runtime optimized by Intel oneAPI is suggested on CPU only cluster.

## Running AI optimized by Intel oneAPI
Intel oneAPI provides many optimizations for achieving the best performance of
AI training and inference on modern hardware.

To use CloudTik machine learning runtime with oneAPI, use the following configuration in the docker section
of your cluster configuration file:

```
docker:
    image: "cloudtik/spark-ai-oneapi"
```

This will use the docker image which is built with oneAPI optimizations for AI.

#!/bin/bash

/home/bfr4xr/3fs/build/bin/storage_bench \
  --clusterMode \
  --clientConfig=/tmp/storage_client_bench.toml \
  --mgmtdEndpoints=RDMA://192.168.100.1:8000 \
  --clusterId=stage \
  --chainTableId=1 \
  --chainIds=1000101001,1000102001 \
  --numChains=2 \
  --numReplicas=1 \
  --chunkSizeKB=4096  \
  --writeSize=4194304 \
  --writeBatchSize=64 \
  --numWriteSecs=30 \
  --numCoroutines=8 \
  --numTestThreads=8 \
  --ibvDevices=mlx5_0 \
  --defaultPKeyIndex=0 \
  --verifyWriteChecksum=false \
  --cleanupChunksBeforeBench \
  --cleanupChunks


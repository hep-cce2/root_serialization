#!/bin/bash
mkdir -p data/test
singularity run \
  -B ${PWD}/data:/data \
  --env MINIO_ROOT_USER=minio \
  --env MINIO_ROOT_PASSWORD=miniotestpass \
  docker://quay.io/minio/minio server /data --console-address ":9001"

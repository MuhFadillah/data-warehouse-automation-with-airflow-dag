#!/bin/bash

export MINIO_ROOT_USER=minioadmin
export MINIO_ROOT_PASSWORD=minioadmin123

./minio server ./data --console-address ":9001" --address ":9000"

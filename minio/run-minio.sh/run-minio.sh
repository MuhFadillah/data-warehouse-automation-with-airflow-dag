#!/bin/bash

export MINIO_ROOT_USER=(your username)
export MINIO_ROOT_PASSWORD=(your password)

./minio server ./data --console-address ":9001" --address ":9000"

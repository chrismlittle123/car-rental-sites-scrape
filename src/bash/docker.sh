#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <image_name>"
    exit 1
fi

IMAGE_NAME="$1"

BUILD_DIR="src/aws_lambda/$(echo $IMAGE_NAME | sed 's/-/_/g')"

docker buildx build -t $IMAGE_NAME --file https://raw.githubusercontent.com/Oxford-Data-Processes/snippets/refs/heads/main/docker/lambda/Dockerfile $BUILD_DIR && \
docker run -p 9000:8080 \
  -e AWS_ACCESS_KEY_ID_ADMIN \
  -e AWS_SECRET_ACCESS_KEY_ADMIN \
  -e STAGE=dev \
  $IMAGE_NAME

#!/bin/sh
set -e
set -x

set -a
source .env

region="us-east-1"
dev_account_id=$(aws sts get-caller-identity --output text|awk '{print $1}')
dev_ecr_endpoint="$dev_account_id.dkr.ecr.$region.amazonaws.com"

repository_name="$dev_ecr_endpoint/ibmlh/presto-shadow-proxy"

aws ecr get-login-password --region "$region" | docker login --username AWS --password-stdin "$dev_ecr_endpoint"

docker context list \
--quiet \
| grep --regexp 'multiarch-build' \
||  docker context create multiarch-build

for architecture in "amd64" "arm64"; do
  DOCKER_BUILDKIT=1 \
  docker buildx build \
  --output=type=docker \
  --platform=linux/$architecture \
  --tag="${repository_name}:latest-linux-${architecture}" \
  .

  docker push "${repository_name}:latest-linux-${architecture}"
done

docker manifest rm "${repository_name}:latest" || true

docker manifest create \
--amend \
"${repository_name}:latest" \
"${repository_name}:latest-linux-amd64" \
"${repository_name}:latest-linux-arm64"

docker manifest inspect "${repository_name}:latest"

docker manifest push "${repository_name}:latest"

set +e
set +x

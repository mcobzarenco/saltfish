#!/bin/bash
set -e -v

REPO=saltfish
REGISTRY=registry.reinfer.io

COMMIT_SHA=$(git log -n 1 --pretty=format:"%H" | cut -c-12)
IMAGE_SHA=$REGISTRY/$REPO:$COMMIT_SHA
IMAGE_LATEST=$REGISTRY/$REPO:latest

sudo docker build -t $IMAGE_SHA .
sudo docker tag -f $IMAGE_SHA $IMAGE_LATEST

sudo docker push $REGISTRY/$REPO

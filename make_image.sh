#!/bin/bash
set -e -v

REPO=saltfish
REGISTRY=registry2.reinfer.io

COMMIT_SHA=$(git log -n 1 --pretty=format:"%H" | cut -c-12)
IMAGE_SHA=$REGISTRY/$REPO:$COMMIT_SHA
IMAGE_LATEST=$REGISTRY/$REPO:latest

sudo docker build --rm=true --no-cache=true -t $IMAGE_SHA .
sudo docker tag $IMAGE_SHA $IMAGE_LATEST

sudo docker push $REGISTRY/$REPO

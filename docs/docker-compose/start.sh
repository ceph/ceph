#!/bin/bash
# Clone paddles and teuthology

git clone https://github.com/ceph/paddles.git
cd paddles
cd ../
git clone https://github.com/ceph/teuthology.git

# Check for .teuthology.yaml file and copy it to teuthology
if [ -f ".teuthology.yaml" ]; 
then
    cp .teuthology.yaml teuthology/.
else
    echo ".teuthology.yaml doesn't exists"
    exit 1
fi

# Copy Docker file into teuthology
cp ./Dockerfile teuthology/.

# docker-compose
docker-compose up --build

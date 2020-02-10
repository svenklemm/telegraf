#!/bin/bash

if [ -d ./build ];
then
	echo "./build already exists, remove it manually"
	exit 1
fi


echo "Building docker container that will build the artifacts"
docker build --tag=telegraf_builder_image .

echo "Starting container"
docker run --name telegraf_builder_container -d telegraf_builder_image 

echo "Copying build directory containing all artifacts to ./build"
docker cp telegraf_builder_container:/go/src/github.com/influxdata/telegraf/build ./

echo "Killing container telegraf_builder_container"
docker kill telegraf_builder_container

echo "Removing container telegraf_build_container" 
docker rm telegraf_builder_container

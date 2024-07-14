#!/bin/bash

docker stop zookeeper kafka cassandra redis
docker rm zookeeper kafka cassandra redis

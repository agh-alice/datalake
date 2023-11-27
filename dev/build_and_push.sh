#!/bin/bash

docker build -t nowickib/spark-executor:latest ../images/spark-executor && docker push nowickib/spark-executor:latest
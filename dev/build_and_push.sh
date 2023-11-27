#!/bin/bash
repo=nowickib
image=airflow
docker build -t $repo/$image:latest .. -f ../images/$image/Dockerfile && docker push $repo/$image:latest
#!/bin/bash

docker cp ./data/dataset.csv namenode:/ && docker exec namenode hdfs dfs -put /dataset.csv /neo/

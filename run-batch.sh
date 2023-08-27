#!/bin/bash

docker exec -it spark-master \
  ./spark/bin/spark-submit \
  --packages org.postgresql:postgresql:42.4.0 \
  /spark/run.py
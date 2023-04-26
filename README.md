# neo-data-analysis

Near-Earth objects data analysis, data engineering project for Big Data processing

## data source used for batch processing:

Asteroid dataset:
https://www.kaggle.com/datasets/sakhawat18/asteroid-dataset

Small body database:
https://ssd.jpl.nasa.gov/tools/sbdb_query.html#!#results

<br>

## data source used for streaming processing:

NeoWs API:
https://www.neowsapp.com/swagger-ui/index.html#/1.%20Date%20Rest%20Service/retrieveNearEarthObjectFeed

NASA API:
https://web.archive.org/web/20190811101743/https://api.nasa.gov/api.html

<br>

### Example:

https://www.101computing.net/real-time-asteroid-watch/

<br>

# starting the project

Download and extract dataset from https://www.kaggle.com/datasets/sakhawat18/asteroid-dataset

## Import data to the HDFS

```
# Copy the dataset to the namenode container
$ docker cp ./data/dataset.csv namenode:/

# Put the dataset in the HDFS
$ docker exec namenode hdfs dfs -put /dataset.csv /neo/
```

## Run python script (run.py)

```
# Enter the spark-master container
$ docker exec -it spark-master /bin/bash

# Position yourself in the spark directory
$ cd /spark/bin

# Run the script
$ ./spark-submit --packages org.postgresql:postgresql:42.4.0 /spark/batch_processor/run.py
```

# 🪐 Docker Image and Container for Registry Loader

The Registry Loader is a docker image which contains both Harvest and Registry Manager command line tools.

## 🏃 Steps to build the docker image of the Registry Loader

1. [Optional] Update the following versions in the Dockerfile with compatible versions.

| Variable            | Description |
| ------------------- | ------------|
| harvest.version     | The version of the Harvest release to be included in the docker image|
| reg.manager.version | The version of the Registry Manager release to be included in the docker image|

```    
    # Set following arguments with compatible versions
    ARG harvest_version=3.5.0
    ARG reg_manager_version=4.2.0
```

2. Build the docker image as follows.

```
    docker image build -t pds/registry-loader .
```

## 🏃 Steps to run a docker container of the Registry Loader

1. Update the following environment variables in the run.sh.

| Variable          | Description |
| ----------------- | ------------|
| ES_URL            | The Elasticsearch URL |
| HARVEST_CFG_FILE  | Absolute path of the Harvest configuration file in the host machine (E.g.: /tmp/cfg/harvest-config.xml) |
| HARVEST_DATA_DIR  | Absolute path of the Harvest data directory in the host machine (E.g.: /tmp/data/urn-nasa-pds-insight_rad) |

```    
# Update the following environment variables before executing this script

# Elasticsearch URL
ES_URL=http://elasticsearch:9200

# Absolute path of the Harvest configuration file in the host machine (E.g.: /tmp/cfg/harvest-config.xml)
HARVEST_CFG_FILE=${PWD}/test/cfg/harvest-test-config.xml

# Absolute path of the Harvest data directory in the host machine (E.g.: /tmp/data/urn-nasa-pds-insight_rad)
HARVEST_DATA_DIR=/tmp/data
```

2. If executing for the first time, change the execution permissions of run.sh file as follows.

```
chmod u+x run.sh
```

3. Execute the run.sh as follows.

```
./run.sh
```

Above steps will run a docker container of the Registry Loader.

## 🏃 Steps to run a docker container of the Registry Loader with test data

1. Update the following environment variable in the run.sh.

| Variable          | Description |
| ----------------- | ------------|
| TEST_DATA_URL     | URL to download the test data to Harvest |

```    
# Update the following environment variable before executing this script

# URL to download the test data to Harvest (only required, if executing with test data)
TEST_DATA_URL=https://pds-gamma.jpl.nasa.gov/data/pds4/test-data/registry/urn-nasa-pds-insight_rad.tar.gz
```

2. If executing for the first time, change the execution permissions of run.sh file as follows.

```
chmod u+x run.sh
```

3. Execute the run.sh with the argument 'test' as follows.

```
./run.sh test
```
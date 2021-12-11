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

1. Update the following variables in the run.sh.

| Variable          | Description |
| ----------------- | ------------|
| ES_URL            | The Elasticsearch URL|
| HARVEST_CFG_FILE  | Harvest configuration file path (in the container)|
| HARVEST_CFG_DIR   | Harvest configuration directory in the host machine, to be mapped as /cfg directory in the container|
| HARVEST_DATA_DIR  |Harvest data directory in the host machine, to be mapped as /data directory in the container|
| NETWORK_NAME      |Name of the network, if this container is located in a docker network with other components such as Elasticsearch|

```    
    # Configure the following variables before executing this script
    ES_URL=http://elasticsearch:9200
    HARVEST_CFG_FILE=/cfg/dir1.xml
    HARVEST_CFG_DIR=/<absolute_path_in_host>/cfg
    HARVEST_DATA_DIR=<absolute_path_in_host>/data
    NETWORK_NAME=pds
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
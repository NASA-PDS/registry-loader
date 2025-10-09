# 🪐 Docker Image and Container for Registry Loader

The Registry Loader is a docker image which contains both Harvest and the Registry Manager command line tools.

## 🏃 Steps to build the docker image of the Registry Loader

#### 1. Update (if required) the following versions in the `Dockerfile` with compatible versions.

| Variable            | Description |
| ------------------- | ------------|
| HARVEST_PACKAGE_PATH     | The URL or local path where harvest release package can be found |
| REG_MANAGER_PACKAGE_PATH | The URL or local path where registry-mgr release package can be found |

```    
# Set following arguments with compatible versions
export HARVEST_PACKAGE_PATH=./harvest/target/harvest-5.0.0-SNAPSHOT-bin.tar.gz
export REG_MANAGER_PACKAGE_PATH=./manager/target/registry-manager-6.0.0-SNAPSHOT-bin.tar.gz
```

#### 2. Open a terminal and change the current working directory to `registry-loader/docker`.

#### 3. Build the docker image as follows.

From the base directory of the project:


```
    docker image build -t nasapds/registry-loader -f docker/Dockerfile --build-arg harvest_package_path=harvest/target/harvest-5.0.0-SNAPSHOT-bin.tar.gz --build-arg reg_manager_package_path=manager/target/registry-manager-6.0.0-SNAPSHOT-bin.tar.gz .
```

## 🏃 Steps to run a docker container of the Registry Loader

#### 1. Update the following environment variables in the `run.sh`.

Note: 

Copies of `harvest-job-config.xml` and `es-auth.cfg` can be obtained from 
https://github.com/NASA-PDS/registry/tree/main/docker/default-config. Please change the Elasticsearch URL, username and 
password to match with your environment.

| Variable            | Description |
| ------------------- | ----------- |
| ES_URL              | The Elasticsearch URL (E.g.: `https://192.168.0.1:9200`) |
| HARVEST_CFG_FILE    | Absolute path of the Harvest configuration file in the host machine (E.g.: `/tmp/cfg/harvest-config.xml`) |
| HARVEST_DATA_DIR    | Absolute path of the Harvest data directory in the host machine (E.g.: `/tmp/data/urn-nasa-pds-insight_rad`) |
| ES_AUTH_CONFIG_FILE | Absolute path of the es-auth.cfg file, which contains elasticsearch authentication details (E.g.: `/tmp/cfg/es-auth.cfg`) |

```    
# Update the following environment variables before executing this script

# Elasticsearch URL (E.g.: https://192.168.0.1:9200)
ES_URL=https://<HOST NAME OR IP ADDRESS>:9200

# Absolute path of the Harvest job configuration file in the host machine (E.g.: /tmp/cfg/harvest-job-config.xml)
HARVEST_CFG_FILE=/tmp/cfg/harvest-job-config.xml

# Absolute path of the Harvest data directory in the host machine (E.g.: /tmp/data/urn-nasa-pds-insight_rad)
HARVEST_DATA_DIR=/tmp/data

# Absolute path of the es-auth.cfg file, which contains elasticsearch authentication details (E.g.: /tmp/cfg/es-auth.cfg)
ES_AUTH_CONFIG_FILE=/tmp/cfg/es-auth.cfg
```

#### 2. Open a terminal and change the current working directory to `registry-loader/docker`.

#### 3. If executing for the first time, change the execution permissions of `run.sh` file as follows.

```
chmod u+x run.sh
```

#### 4. Execute the `run.sh` as follows.

```
./run.sh
```

Above steps will run a docker container of the Registry Loader.

## 🏃 Steps to run a docker container of the Registry Loader with test data

#### 1. Update the following environment variable in the `run.sh`.

| Variable          | Description |
| ----------------- | ----------- |
| TEST_DATA_URL     | URL to download the test data to harvest |
| TEST_DATA_LIDVID  | The lidvid of the test data, which is used to set the archive status |

```    
# Update the following environment variables before executing this script

# URL to download the test data to Harvest (only required, if executing with test data)
TEST_DATA_URL=https://pds-gamma.jpl.nasa.gov/data/pds4/test-data/registry/urn-nasa-pds-insight_rad.tar.gz

# The lidvid of the test data, which is used to set the archive status (only required, if executing with test data)
TEST_DATA_LIDVID=urn:nasa:pds:insight_rad::2.1
```

#### 2. Open a terminal and change the current working directory to `registry-loader/docker`.

#### 3. If executing for the first time, change the execution permissions of `run.sh` file as follows.

```
chmod u+x run.sh
```

#### 4. Execute the `run.sh` with the argument `test` as follows.

```
./run.sh test
```

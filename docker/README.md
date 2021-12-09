# 🪐 Building a Docker Image for Registry Loader

The Registry Loader is a docker image which contains both Harvest and Registry Manager command line tools.

## 🏃 Steps to build the docker image for Registry Loader

1. Update the values in the docker.properties file.

Currently, the docker.properties file contains following 3 properties. Update these properties with the compatible versions and a preferred docker image name tag.

```
    harvest.version=3.6.0-SNAPSHOT              - The version of the Harvest release to be included in the docker image
    reg.manager.version=4.3.0-SNAPSHOT          - The version of the Registry Manager release to be included in the docker image
    docker.image.name.tag=pds/registry-loader   - The image name tag to be used to identify the docker image
```



2. Change the execution permission of the build.sh file as follows.

```
    chmod u+x build.sh
```


3. Execute the build.sh with docker.properties file passed as an argument.

```
    ./build.sh docker.properties
```

Above steps will build a docker image. This docker image will have the name tag, which was specified as docker.image.name.tag in the docker.properties file.

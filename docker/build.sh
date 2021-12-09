#!/usr/bin/env bash

# Copyright 2021, California Institute of Technology ("Caltech").
# U.S. Government sponsorship acknowledged.
#
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice,
# this list of conditions and the following disclaimer.
# * Redistributions must reproduce the above copyright notice, this list of
# conditions and the following disclaimer in the documentation and/or other
# materials provided with the distribution.
# * Neither the name of Caltech nor its operating division, the Jet Propulsion
# Laboratory, nor the names of its contributors may be used to endorse or
# promote products derived from this software without specific prior written
# permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

# ----------------------------------------------------------------------------------------------
# This script is used to build the docker image for the Registry Loader with a single command.
#
# Both Harvest and Registry Manager tools are included in this Registry Loader docker image.
# This script reads required configurations from a properties file passed as an argument.
#
# Usage: ./build.sh docker.properties
#
# ----------------------------------------------------------------------------------------------

# Check if a properties file name is provided as an argument to this script
if [ -z "$1" ]; then
  echo -e "\nPlease provide a properties file as an argument to the $0" 1>&2
  echo -e "\tUsage: $0 docker.properties \n" 1>&2
  exit 1
fi

PROPERTY_FILE=$1

# Check if the provided properties file exists
if [ ! -f "$PROPERTY_FILE" ]; then
    echo -e "\nThe file $PROPERTY_FILE does not exist." 1>&2
    echo -e "\tPlease provide an existing properties file as an argument to the $0" 1>&2
    echo -e "\tUsage: $0 docker.properties \n" 1>&2
    exit 1
fi

# Returns the value for a given property key
function getProperty {
   PROPERTY_KEY=$1
   PROPERTY_VALUE=$(grep -w "$PROPERTY_KEY" < "$PROPERTY_FILE" | cut -d '=' -f2)
   echo "$PROPERTY_VALUE"
}

# Read property values from the properties file
echo "Reading properties from $PROPERTY_FILE" 1>&2
HARVEST_VERSION=$(getProperty "harvest.version")
REG_MANAGER_VERSION=$(getProperty "reg.manager.version")
DOCKER_IMAGE_NAME_TAG=$(getProperty "docker.image.name.tag")

echo "HARVEST_VERSION  = $HARVEST_VERSION" 1>&2
echo "REG_MANAGER_VERSION = $REG_MANAGER_VERSION" 1>&2
echo "DOCKER_IMAGE_NAME_TAG = $DOCKER_IMAGE_NAME_TAG" 1>&2

# Execute docker image build with build arguments
docker image build --build-arg harvest_version="$HARVEST_VERSION" \
             --build-arg reg_manager_version="$REG_MANAGER_VERSION" \
             --tag "$DOCKER_IMAGE_NAME_TAG" \
             --file Dockerfile .

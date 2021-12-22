#!/bin/sh

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
# This script is used to run the Registry Loader docker container with a single command.
#
# Usage: ./run.sh [test]
#
# Optional arguments:
#     test     Download and harvest test data
#
# ----------------------------------------------------------------------------------------------

# Update the following environment variables before executing this script

# Elasticsearch URL (E.g.: http://192.168.0.1:9200)
ES_URL=http://<HOST NAME OR IP ADDRESS>:9200

# Absolute path of the Harvest configuration file in the host machine (E.g.: /tmp/cfg/harvest-config.xml)
HARVEST_CFG_FILE=/tmp/cfg/harvest-config.xml

# Absolute path of the Harvest data directory in the host machine (E.g.: /tmp/data/urn-nasa-pds-insight_rad)
HARVEST_DATA_DIR=/tmp/data

# URL to download the test data to Harvest (only required, if executing with test data)
TEST_DATA_URL=https://pds-gamma.jpl.nasa.gov/data/pds4/test-data/registry/urn-nasa-pds-insight_rad.tar.gz



# Check if the ES_URL environment variable is set
if [ -z "$ES_URL" ]; then
    echo "Error: 'ES_URL' (Elasticsearch URL) environment variable is not properly set in the $0 file." 1>&2
    exit 1
fi

# Check if an argument is provided to this script
if [ -z "$1" ]; then

      # If an argument is not provided, then do not harvest test data. Execute the registry loader with actual
      # configurations and data provided with 'HARVEST_CFG_FILE' and 'HARVEST_DATA_DIR' environment variables
      # in this file.

      # Check if the Harvest configuration file exists
      if [ ! -f "$HARVEST_CFG_FILE" ]; then
          echo -e "Error: The Harvest configuration file $HARVEST_CFG_FILE does not exist." \
                  "Set an absolute file path of an existing Harvest configuration file in the $0 file" \
                  "as the environment variable 'HARVEST_CFG_FILE'.\n" 1>&2
          exit 1
      fi

      # Check if the Harvest configuration file exists
      if [ ! -d "$HARVEST_DATA_DIR" ]; then
          echo -e "Error: The Harvest data directory $HARVEST_DATA_DIR does not exist." \
                  "Set an absolute directory path of an existing Harvest data directory in the $0 file" \
                  "as the environment variable 'HARVEST_DATA_DIR'.\n" 1>&2
          exit 1
      fi

      # Execute docker container run with actual data
      docker container run --name registry-loader \
                 --rm \
                 --env ES_URL=${ES_URL} \
                 --volume "${HARVEST_CFG_FILE}":/cfg/harvest-config.xml \
                 --volume "${HARVEST_DATA_DIR}":/data \
                 pds/registry-loader
else

    if [ "$1" = "test" ]; then

      # Execute docker container run with test data
      docker container run --name registry-loader \
                 --rm \
                 --env ES_URL="${ES_URL}" \
                 --env RUN_TESTS=true \
                 --env TEST_DATA_URL="${TEST_DATA_URL}" \
                 pds/registry-loader

    else
      echo -e "Usage: $0 [test]\n" 1>&2
      echo -e "Optional argument:" 1>&2
      echo -e "\ttest     Download and harvest test data\n" 1>&2
      echo -e "Execute $0 without any arguments to harvest with actual configurations and data provided" \
              "with 'HARVEST_CFG_FILE' and 'HARVEST_DATA_DIR' environment variables set in the $0 file.\n" 1>&2
      exit 1
    fi
fi

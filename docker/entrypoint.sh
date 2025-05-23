#!/bin/sh

# Copyright 2022, California Institute of Technology ("Caltech").
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

# ------------------------------------------------------------------------------
# This shell script provides an entrypoint for the registry loader docker image.
# ------------------------------------------------------------------------------

ES_CONNECTION_FILE=file:///test/cfg/connection.xml

# Harvest data
if [ "$RUN_TESTS" = "true" ]; then
  echo "Downloading Harvest test data ..." 1>&2
  curl -L -o /tmp/data.tar.gz "$TEST_DATA_URL"
  rm -fr /data/*
  mkdir -p /data && tar xzf /tmp/data.tar.gz -C /data --strip-components 1
  rm -f /tmp/data.tar.gz
  HARVEST_CFG_FILE=/test/cfg/harvest-config.xml
else
  HARVEST_CFG_FILE=/cfg/harvest-config.xml
fi

echo "Harvesting data based on the configuration file: $HARVEST_CFG_FILE ..." 1>&2
harvest -c "$HARVEST_CFG_FILE" --overwrite

if [ "$RUN_TESTS" = "true" ]; then
  echo "Setting archive status ..." 1>&2
  for lid in $TEST_DATA_LIDVID
  do
      registry-manager set-archive-status -status archived -lidvid "$lid" -es "$ES_CONNECTION_FILE"  -auth /etc/es-auth.cfg
  done
  registry-manager set-archive-status -status staged -lidvid "urn:nasa:pds:mars2020.spice:document::1.0" -es "$ES_CONNECTION_FILE"  -auth /etc/es-auth.cfg
  registry-manager set-archive-status -status archived -lidvid "urn:nasa:pds:insight_rad::2.1" -es "$ES_CONNECTION_FILE" -auth /etc/es-auth.cfg
fi

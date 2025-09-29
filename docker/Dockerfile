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

FROM openjdk:11-slim

# Set following arguments with compatible versions
ARG HARVEST_PACKAGE_PATH
ARG REG_MANAGER_PACKAGE_PATH
ARG test_data_url=https://pds.nasa.gov/data/pds4/test-data/registry/custom-datasets.tar.gz


# Install curl
RUN apt-get update -y
RUN apt-get install curl -y


# Install Harvest
ADD $HARVEST_PACKAGE_PATH /tmp/harvest-bin.tar.gz
RUN  mkdir /opt/harvest  \
     && tar xzf /tmp/harvest-bin.tar.gz  -C /opt/harvest --strip-components 1 \
     && rm -f /tmp/harvest-bin.tar.gz
ENV PATH="$PATH:/opt/harvest/bin"

# Install Registry Manager
ADD $REG_MANAGER_PACKAGE_PATH /tmp/registry-manager-bin.tar.gz
RUN  mkdir /opt/registry-manager  \
     && tar xzf /tmp/registry-manager-bin.tar.gz  -C /opt/harvest --strip-components 1 \
     && rm -f /tmp/registry-manager-bin.tar.gz
ENV PATH="$PATH:/opt/registry-manager/bin"

# Copy test data config file \
RUN  mkdir -p /test/cfg
COPY ./src/test/resources//harvest-test-config.xml /test/cfg/harvest-config.xml
COPY ./src/test/resources//connection.xml /test/cfg/connection.xml
COPY ./src/test/resources/es-auth.cfg /test/cfg/es-auth.cfg

# Entry point
COPY ./docker/entrypoint.sh /usr/local/bin/entrypoint.sh
ENTRYPOINT ["bash", "/usr/local/bin/entrypoint.sh"]

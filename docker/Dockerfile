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

FROM eclipse-temurin:25-jdk-jammy

# Set following arguments with compatible versions
ARG harvest_package_path=https://github.com/NASA-PDS/harvest/releases/download/v5.0.0/harvest-5.0.0-bin.tar.gz
ARG reg_manager_package_path=https://github.com/NASA-PDS/registry-mgr/releases/download/v6.0.0/registry-manager-6.0.0-bin.tar.gz
ARG test_data_url=https://pds.nasa.gov/data/pds4/test-data/registry/custom-datasets.tar.gz


# Install curl
RUN apt-get update -y
RUN apt-get install curl -y


# Add and untar application packages
# TODO change when we switch to using the package found in the same repository
RUN mkdir /app
ADD $harvest_package_path /app/
ADD $reg_manager_package_path /app/
# One extraction step for all archives
RUN sh -c 'ls /app/*.tar.gz 2>/dev/null | grep -q . && for f in /app/*.tar.gz; do tar -xzf "$f" -C /app; done || echo "No .tar.gz files to extract"' \    
    && rm -f /app/*.tar.gz

# Harvest in path
RUN ln -s /app/harvest-* /app/harvest
ENV PATH="$PATH:/app/harvest/bin"

# Registry Manager in path
RUN ln -s /app/registry-manager-* /app/registry-manager
ENV PATH="$PATH:/app/registry-manager/bin"

# Copy test data config file \
RUN  mkdir -p /test/cfg
COPY ./src/test/resources//harvest-test-config.xml /test/cfg/harvest-config.xml
COPY ./src/test/resources//connection.xml /test/cfg/connection.xml
COPY ./src/test/resources/es-auth.cfg /test/cfg/es-auth.cfg

# Entry point
COPY ./docker/entrypoint.sh /usr/local/bin/entrypoint.sh
ENTRYPOINT ["bash", "/usr/local/bin/entrypoint.sh"]

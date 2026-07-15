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

FROM eclipse-temurin:25-jdk-jammy AS base

ARG harvest_package_path=https://github.com/NASA-PDS/harvest/releases/download/v5.1.2/harvest-5.1.3-bin.tar.gz
ARG reg_manager_package_path=https://github.com/NASA-PDS/registry-mgr/releases/download/v6.1.2/registry-manager-6.1.3-bin.tar.gz

RUN apt-get update -y
RUN apt-get install curl -y

RUN mkdir /app
RUN curl -L "$harvest_package_path" -o /app/harvest.tar.gz && tar -xzf /app/harvest.tar.gz -C /app && rm /app/harvest.tar.gz
RUN curl -L "$reg_manager_package_path" -o /app/registry-manager.tar.gz && tar -xzf /app/registry-manager.tar.gz -C /app && rm /app/registry-manager.tar.gz
# commented out, this is used for local/manual builds, where the tar.gz files are already downloaded and available in the docker folder
#COPY ./docker/harvest-5.1.3-bin.tar.gz /app/harvest.tar.gz
#COPY ./docker/registry-manager-6.1.3-bin.tar.gz /app/registry-manager.tar.gz
#RUN tar -xzf /app/harvest.tar.gz -C /app && rm /app/harvest.tar.gz
#RUN tar -xzf /app/registry-manager.tar.gz -C /app && rm /app/registry-manager.tar.gz


RUN ln -s /app/harvest-* /app/harvest
ENV PATH="$PATH:/app/harvest/bin"

RUN ln -s /app/registry-manager-* /app/registry-manager
ENV PATH="$PATH:/app/registry-manager/bin"


FROM base AS lite


FROM base AS full

RUN mkdir -p /test/cfg
COPY ./src/test/resources/harvest-test-config.xml /test/cfg/harvest-config.xml
COPY ./src/test/resources/connection.xml /test/cfg/connection.xml
COPY ./src/test/resources/es-auth.cfg /test/cfg/es-auth.cfg

COPY ./docker/entrypoint.sh /usr/local/bin/entrypoint.sh
ENTRYPOINT ["bash", "/usr/local/bin/entrypoint.sh"]

FROM docker.io/library/fedora:32
USER root


RUN yum update --assumeyes

RUN yum install --assumeyes python36
RUN yum install --assumeyes python3-jsonschema
RUN yum install --assumeyes git

RUN yum clean all

RUN pip3 install redis

RUN git clone https://github.com/perftool-incubator/roadblock.git /opt/roadblock

ENTRYPOINT ["/bin/bash"]

FROM docker.io/library/fedora
USER root


RUN yum update --assumeyes

RUN yum install --assumeyes python36
RUN yum install --assumeyes git

RUN yum clean all

RUN pip3 install redis

RUN git clone https://github.com/perftool-incubator/roadblock.git /opt/roadblock

ENTRYPOINT ["/bin/bash"]

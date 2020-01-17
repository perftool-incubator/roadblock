FROM docker.io/library/fedora
USER root


RUN yum update --assumeyes

RUN yum install --assumeyes python36

RUN yum clean all

RUN pip3 install redis


ENTRYPOINT ["/bin/bash"]

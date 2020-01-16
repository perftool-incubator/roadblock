FROM localhost/fedora-redis-python-client
USER root


RUN yum update --assumeyes

RUN yum clean all


ENTRYPOINT ["/bin/bash"]

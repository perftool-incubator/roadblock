FROM localhost/fedora-redis-python-client
USER root


RUN mkdir /opt/roadblock
COPY * /opt/roadblock


ENTRYPOINT ["/bin/bash"]

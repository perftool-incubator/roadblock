FROM localhost/fedora-redis-python-client
USER root


RUN mkdir /opt/roadblock
COPY * /opt/roadblock
COPY utilities/redis-monitor.py /opt/roadblock
COPY utilities/wait-for-script.sh /opt/roadblock
COPY test/user-messages.json /opt/roadblock

ENTRYPOINT ["/bin/bash"]

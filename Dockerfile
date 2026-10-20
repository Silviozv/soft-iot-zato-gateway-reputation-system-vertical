FROM rhianpablo11/esb-zato-soft-iot:v12

USER root

RUN mkdir -p /opt/hot-deploy/myproject \
             /opt/hot-deploy/enmasse \
             /opt/hot-deploy/python-reqs \
             /home/ubuntu/mapping_archives/devices_config/

COPY . /opt/hot-deploy/myproject/

COPY config/enmasse/enmasse.yaml /opt/hot-deploy/enmasse/enmasse.yaml
COPY config/auto-generated/env.ini /opt/hot-deploy/enmasse/env.ini
COPY config/python-reqs/requirements.txt /opt/hot-deploy/python-reqs/requirements.txt
COPY impl/src/archives/. /home/ubuntu/mapping_archives/devices_config/

RUN rm -f /opt/hot-deploy/myproject/impl/scripts/fogbed-test.py

RUN chown -R zato:zato /opt/hot-deploy/ /home/ubuntu/mapping_archives/

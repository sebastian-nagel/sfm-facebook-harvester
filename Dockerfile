FROM gwul/sfm-base:master
# @sha256:e68cb98bdc9dc23bbed734f3e507a0ffb866b007dffea038b6af8d88a62150e6
MAINTAINER Frederik Gremler <frederik.gremler@uni-konstanz.de>

RUN echo "something"

ADD . /opt/sfm-facebook-harvester/
WORKDIR /opt/sfm-facebook-harvester
# RUN pip install -r requirements/common.txt

ADD docker/invoke.sh /opt/sfm-setup/
RUN chmod +x /opt/sfm-setup/invoke.sh
RUN apt-get install jq -yq

CMD ["/opt/sfm-setup/invoke.sh"]

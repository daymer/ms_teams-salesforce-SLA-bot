# Version: 2.0
# docker build -t elisa:2.0 -f /home/drozhd/Elisa/Dockerfile /home/drozhd/Elisa
FROM python:3.6.2
MAINTAINER Dmitry Rozhdestvenskiy <dremsama@gmail.com>
RUN apt-get update && apt-get install -y --no-install-recommends apt-utils \
    && apt-get -y install locales \
    && echo "en_US.UTF-8 UTF-8" > /etc/locale.gen \
    && locale-gen \
    && apt-get -y install apt-transport-https freetds-dev unixodbc-dev git \
    && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/8/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get -y update && ACCEPT_EULA=Y apt-get install msodbcsql \
    && mkdir /Elisa \
    && git clone https://github.com/daymer/ms_teams-salesforce-SLA-bot /Elisa \
    && pip install --upgrade pip \
    && pip install -r /Elisa/requirements.txt \
    && mkdir /var/log/elisa/ \
    && chmod +x /Elisa/launch_Elisa.sh
ADD configuration.py /Elisa/
CMD ["/bin/bash", "/Elisa/launch_Elisa.sh"]
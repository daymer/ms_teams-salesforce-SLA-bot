# Version: 3.0
# docker build -t elisa:1.0 -f /home/drozhd/Elisa/Dockerfile /home/drozhd/Elisa
FROM python:3.6.2
MAINTAINER Dmitry Rozhdestvenskiy <dremsama@gmail.com>
RUN apt-get update && apt-get install -y --no-install-recommends apt-utils
RUN apt-get -y install locales
RUN echo "en_US.UTF-8 UTF-8" > /etc/locale.gen
RUN locale-gen
RUN apt-get -y install apt-transport-https freetds-dev unixodbc-dev git
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/debian/8/prod.list > /etc/apt/sources.list.d/mssql-release.list
RUN apt-get -y update && ACCEPT_EULA=Y apt-get install msodbcsql
RUN mkdir /Elisa
RUN git clone https://github.com/daymer/ms_teams-salesforce-SLA-bot /Elisa
RUN pip install --upgrade pip
RUN pip install -r /Elisa/requirements.txt
RUN mkdir /var/log/elisa/
ADD configuration.py /Elisa/
RUN chmod +x /Elisa/launch_Elisa.sh
CMD ["/bin/bash", "/Elisa/launch_Elisa.sh"]
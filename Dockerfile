FROM ubuntu:16.04

RUN \
 apt-get update && \
 apt-get install -y gcc unixodbc unixodbc-dev freetds-dev freetds-bin tdsodbc vim python3 python3-pip && \
 pip3 install --upgrade pip && \
 pip3 install --no-binary pymssql pymssql


ADD Logistic_Parse /root/Logistic_Parse

RUN cd /root/Logistic_Parse && pip3 install -r requirements.txt

#RUN \
# pip3 install -r requirements.txt

#RUN cd /root/Logistic_Parse && \
#    pip3 install -r requirements.txt --no-index

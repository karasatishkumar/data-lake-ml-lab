#ARG ubuntu_version=18.04
#FROM ubuntu:${ubuntu_version}
#Use ubuntu 18:04 as your base image
FROM ubuntu:18.04
#Any label to recognise this image.
LABEL image=Spark-base-image
ENV SPARK_VERSION=2.4.1
ENV HADOOP_VERSION=2.7
#Run the following commands on my Linux machine
#install the below packages on the ubuntu image
RUN apt-get update -qq && \
    apt-get install -qq -y gnupg2 wget openjdk-8-jdk scala
#Download the Spark binaries from the repo
WORKDIR /
RUN wget --no-verbose http://www.gtlib.gatech.edu/pub/apache/spark/spark-2.4.4/spark-2.4.4-bin-hadoop2.7.tgz
# Untar the downloaded binaries , move them the folder name spark and add the spark bin on my class path
RUN tar -xzf /spark-2.4.4-bin-hadoop2.7.tgz && \
    mv spark-2.4.4-bin-hadoop2.7 spark && \
    echo "export PATH=$PATH:/spark/bin" >> ~/.bashrc
#Expose the UI Port 4040
EXPOSE 4040

FROM ubuntu:15.04

RUN apt-get update
RUN apt-get install -y python-pip python-virtualenv git

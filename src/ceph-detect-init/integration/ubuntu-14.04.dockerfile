FROM ubuntu:14.04

RUN apt-get update
# http://stackoverflow.com/questions/27341064/how-do-i-fix-importerror-cannot-import-name-incompleteread
RUN apt-get install -y python-setuptools && easy_install -U pip
RUN apt-get install -y python-virtualenv git

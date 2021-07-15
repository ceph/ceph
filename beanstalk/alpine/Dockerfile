# For beanstalkd 1.12 use edge branch
#FROM alpine:edge

FROM alpine:3.12.3

MAINTAINER Kyrylo Shatskyy <kyrylo.shatskyy@suse.com>

RUN apk update && apk add beanstalkd beanstalkd-doc

ENV BEANSTALK_ADDR "0.0.0.0"
ENV BEANSTALK_PORT "11300"

CMD /usr/bin/beanstalkd -V -l $BEANSTALK_ADDR -p $BEANSTALK_PORT

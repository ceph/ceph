FROM ubuntu:18.04
LABEL maintainer="dupadhya@redhat.com"

# general utilities
RUN apt-get update && apt-get install -y \
    bash vim git wget curl zsh  
RUN apt-get install -y apt-utils gcc make cmake \
    software-properties-common sudo gdb  

# jaeger-dependencies
RUN apt-get install -y apt-utils libyaml-cpp-dev libgtest-dev \
    nlohmann-json-dev libboost-dev libboost-test-dev \
    libboost-program-options-dev libboost-filesystem-dev \
    libboost-thread-dev libevent-dev automake libtool flex \
    bison pkg-config g++ libssl-dev nlohmann-json-dev \
    python3-setuptools

RUN sudo ln -s /usr/lib/x86_64-linux-gnu/libboost_unit_test_framework.a \
    /usr/local/lib/libboost_unit_test_framework.a

# jaeger dependencies that needs build from source 
RUN wget http://archive.apache.org/dist/thrift/0.11.0/thrift-0.11.0.tar.gz \
    && tar -xzf thrift-0.11.0.tar.gz \
    && cd thrift-0.11.0 \
    && ./bootstrap.sh \
    && ./configure --with-boost=/usr/local \
    && make -j$(nproc) \
    && sudo make install


#temporary will be available by distribution pkg
RUN git clone https://github.com/nlohmann/json.git \
    && cd json \
    && mkdir build \
    && cd build \
    && cmake -DBUILD_SHARED_LIBS=ON .. \
    && make -j$(nproc) \
    && sudo make install 

RUN git clone https://github.com/opentracing/opentracing-cpp.git \
    && cd opentracing-cpp \
    && git checkout 1.5.x \
    && mkdir .build \
    && cd .build \
    && cmake -DBUILD_SHARED_LIBS=ON -DCMAKE_POSITION_INDEPENDENT_CODE=ON .. \
    && make -j$(nproc) \
    && sudo make install

RUN git clone https://github.com/jaegertracing/jaeger-client-cpp.git \
    && cd jaeger-client-cpp \
    && mkdir build \
    && cd build \
    && cmake -DBUILD_TESTING=OFF -DHUNTER_ENABLED=OFF .. \
    && make -j$(nproc) \
    && sudo make install
   
ADD /shared/docker/ /docker

# oh-my-zsh
ENV ZSH_DISABLE_COMPFIX true
RUN wget https://github.com/robbyrussell/oh-my-zsh/raw/master/tools/install.sh -O - | zsh || true
ENV CEPH_ROOT /ceph

VOLUME ["/ceph"]
VOLUME ["/shared"]

CMD ["zsh"]

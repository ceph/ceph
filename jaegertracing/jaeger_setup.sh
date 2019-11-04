WORKSPACE="$(dirname "$(pwd)")"

NAME="${NAME:-ceph-container-0}"
CEPH="${CEPH:-$WORKSPACE/ceph}"
CCACHE="${CCACHE:-$WORKSPACE/ceph-ccache}"
VERSION="${VERSION:-master}"

# Removes old container with same name
docker stop $NAME
docker rm $NAME
# For Jaeger frontend
docker stop jaeger
docker rm jaeger 

# Creates a container with all recommended configs
docker run -itd \
  -v $CEPH:/ceph \
  -v $CCACHE:/root/.ccache \
  -v $(pwd)/shared:/shared \
  -v /run/udev:/run/udev:ro \
  --privileged \
  --cap-add=SYS_PTRACE \
  --security-opt seccomp=unconfined \
  --net=host \
  --name=$NAME \
  --hostname=$NAME \
  --add-host=$NAME:127.0.0.1 \
  $TAG

# Running the Jaeger frontend
docker run -d --name jaeger \
  -e COLLECTOR_ZIPKIN_HTTP_PORT=9411 \
  -p 5775:5775/udp \
  -p 6831:6831/udp \
  -p 6832:6832/udp \
  -p 5778:5778 \
  -p 16686:16686 \
  -p 14268:14268 \
  -p 9411:9411 \
  jaegertracing/all-in-one:1.12

docker exec -it $NAME zsh
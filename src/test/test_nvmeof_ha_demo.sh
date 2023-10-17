pushd ../src/nvmeof/gateway
make down
popd
../src/stop.sh
../src/vstart.sh --new --without-dashboard --memstore
./bin/ceph osd pool create rbd 
./bin/ceph osd pool application enable rbd rbd
./bin/rbd -p rbd create demo_image1 --size 10M
./bin/rbd -p rbd create demo_image2 --size 10M
pushd ../src/nvmeof/gateway
docker-compose up -d --scale nvmeof=2 nvmeof
for i in $(seq 2); do
    while true; do
        sleep 1  # Adjust the sleep duration as needed
        GW_NAME=$(docker ps --format '{{.ID}}\t{{.Names}}' | awk '$2 ~ /nvmeof/ && $2 ~ /'$i'/ {print $1}')
        container_status=$(docker inspect -f '{{.State.Status}}' "$GW_NAME")
        if [ "$container_status" == "running" ]; then
           echo "Container $i $GW_NAME is now running."
        else
           echo "Container $i $GW_NAME is still not running. Waiting..."
	   continue
        fi
        GW_IP="$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$GW_NAME")"
        if docker-compose run --rm nvmeof-cli --server-address $GW_IP --server-port 5500 get_subsystems 2>&1 | grep -i failed; then
           echo "Container $i $GW_NAME $GW_IP no subsystems. Waiting..."
	   continue
	fi
        echo "Container $i $GW_NAME $GW_IP subsystems:"
        docker-compose run --rm nvmeof-cli --server-address $GW_IP --server-port 5500 get_subsystems
	break;
    done
done
docker ps
GW1_NAME=$(docker ps --format '{{.ID}}\t{{.Names}}' | awk '$2 ~ /nvmeof/ && $2 ~ /1/ {print $1}')
GW2_NAME=$(docker ps --format '{{.ID}}\t{{.Names}}' | awk '$2 ~ /nvmeof/ && $2 ~ /2/ {print $1}')
GW1_IP="$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$GW1_NAME")"
GW2_IP="$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$GW2_NAME")"
NQN=$("nqn.2016-06.io.spdk:cnode1")
docker-compose  run --rm nvmeof-cli --server-address $GW1_IP --server-port 5500 subsystem add --subsystem  "nqn.2016-06.io.spdk:cnode1" -a -t
docker-compose  run --rm nvmeof-cli --server-address $GW1_IP --server-port 5500  namespace add --subsystem "nqn.2016-06.io.spdk:cnode1" --rbd-pool rbd --rbd-image demo_image1 --size 10M --rbd-create-image -l 1
docker-compose  run --rm nvmeof-cli --server-address $GW1_IP --server-port 5500  namespace add --subsystem "nqn.2016-06.io.spdk:cnode1" --rbd-pool rbd --rbd-image demo_image2 --size 10M --rbd-create-image -l 2
docker-compose  run --rm nvmeof-cli --server-address $GW1_IP --server-port 5500   listener add --subsystem "nqn.2016-06.io.spdk:cnode1" --gateway-name $GW1_NAME --traddr $GW1_IP --trsvcid 4420
docker-compose  run --rm nvmeof-cli --server-address $GW2_IP --server-port 5500   listener add --subsystem "nqn.2016-06.io.spdk:cnode1" --gateway-name $GW2_NAME --traddr $GW2_IP --trsvcid 4420
docker-compose  run --rm nvmeof-cli --server-address $GW1_IP --server-port 5500   host add     --subsystem "nqn.2016-06.io.spdk:cnode1" --host "*"
docker-compose  run --rm nvmeof-cli --server-address $GW1_IP --server-port 5500 subsystem list
docker-compose  run --rm nvmeof-cli --server-address $GW2_IP --server-port 5500 subsystem list
popd
nvme disconnect-all

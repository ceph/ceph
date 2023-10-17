test_dir=$(dirname $0)
source $test_dir/test_nvmeof_ha_demo.sh

# Function to generate a random number between 1 and a given maximum
generate_random_number() {
  local max=$1
  echo $(shuf -i 1-$max -n 1)
}

random_sleep_secs=$(generate_random_number 60) # no more than a min
echo "Sleeping a random number of seconds: $random_sleep_secs"
sleep $random_sleep_secs
echo "Stopping $GW1_NAME $GW1_IP"
docker stop $GW1_NAME
ITERATIONS=120
EXPECTED="Ready for blocklist osd map epoch"
for i in $(seq $ITERATIONS); do
    if docker logs $GW2_NAME 2>&1 | grep "$EXPECTED"; then
        echo "READY"
	docker logs $GW2_NAME 2>&1 | grep "$EXPECTED"
	exit 0
    fi
    echo "Waiting for $i secs for: \"$EXPECTED\" on $GW2_NAME $GW2_IP"
    sleep 1
done
echo "FAILED"
exit 1


test_dir=$(dirname $0)
source $test_dir/test_nvmeof_ha_demo.sh

# Function to generate a random number between 1 and a given maximum
generate_random_number() {
  local max=$1
  echo $(shuf -i 1-$max -n 1)
}

random_sleep_secs=$(generate_random_number 40) # no more than a min
echo "Sleeping a random number of seconds: $random_sleep_secs"
sleep $random_sleep_secs

EXPECTED="Ready for blocklist osd map epoch"
for i in {1..100} ;  do
   echo "Stopping $GW1_NAME $GW1_IP"
   docker stop $GW1_NAME
   sleep 30
   docker logs $GW2_NAME 2>&1 | grep "$EXPECTED"
   echo "Starting $GW1_NAME $GW1_IP"
   docker start $GW1_NAME
   sleep 25
   docker logs $GW1_NAME 2>&1 | grep "$EXPECTED"
   
 done



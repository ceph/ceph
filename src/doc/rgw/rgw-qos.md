# RGW Quality of Service
## Requirements

 - All RGWs should be aware how many requests an user or a bucket have been made per second
	 - There should be a synchronizing implementation to achieve it.
	 -  In later step, RGW will be able to aggregate throughput metrics
	 - The OP/s should be separated by read and write operations while stored in the data structure
	 - RGWs should be aware which RGWs to synchronize from (What are the zmq publishers in the cluster)
 - RGW should implement data structure that can contain the synchronized data from other RGWs (with incrementing) and can be reset each interval for example each second
 - RGW should use user and bucket system objects to contain QoS information (For example, limit this user to 1000 op/s, limit that bucket to 100 op/s)

  ## Synchronizing performance metrics between RGWs
  Zeromq(zmq) will be used as a pubsub library to send the performance metrics (op/s or MB/s, per bucket and user) between RGWs.
  This implementation will use pubsub component of the zmq, while each RGW will be publisher to send user and bucket op/s and subscribe to all RGWs to get their metrics.
 Metrics will be aggregated, that way it will minimize the network traffic, RGW will not send message for each request but each 100ms for example.

## Finding which RGWs are in the cluster

There could be 2 possible ways to implement it:

 1. Declare all RGWs' zmq endpoints in the conf separated by commas and connect to them
 2. Use service map to get the publisher endpoint of each RGW in the cluster and connect to it (could be implemented later)

## Tasks

 1. Add zmq publisher and subscriber to all RGW, both in separated threads and use config to mention all publishers in the cluster (RGW bind address) (second option is to use service map to detect the publisher network)
 2. Add data structure to handle the usage metrics for user and bucket op/s before sending with the pubsub other RGWs
 3. Add data structure to store aggregated metrics to check if the tenant has reached its quota
 4. Clear the aggregated data structure each time interval
 5. Update aggregated data structure for each message gotten from the publisher
 6. Check each request if it has reached the limits or not and fail the request if it does


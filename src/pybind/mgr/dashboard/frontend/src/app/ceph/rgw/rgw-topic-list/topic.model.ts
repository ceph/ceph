interface TopicDest {
  push_endpoint: string;
  push_endpoint_args: string;
  push_endpoint_topic: string;
  stored_secret: boolean;
  persistent: boolean;
  persistent_queue: string;
  time_to_live: number;
  max_retries: number;
  retry_sleep_duration: number;
}

export interface TopicModel {
  owner: string;
  name: string;
  arn: string;
  dest: TopicDest;
  opaqueData: string;
  policy: string | {};
  subscribed_buckets: any[];
}

export interface ApiResponse {
  topics?: TopicModel[];
}

export const END_POINT_TYPE = {
  Select: 'Select Endpoint Type',
  HTTP: 'HTTP',
  AMQP: 'AMQP',
  Kafka: 'KAFKA'
};
export const AMQP_ACK_LEVEL = {
  Select: 'Select AMQP level',
  none: 'none',
  broker: 'broker',
  routable: 'routable'
};
export const KAFKA_ACK_LEVEL = {
  Select: 'Select KAFKA level',
  none: 'none',
  broker: 'broker'
};

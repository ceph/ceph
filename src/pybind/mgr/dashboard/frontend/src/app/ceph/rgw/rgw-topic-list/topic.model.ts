interface TopicDest {
  push_endpoint: string;
  push_endpoint_args: string;
  push_endpoint_topic: string;
  stored_secret: boolean;
  persistent: boolean;
  persistent_queue: string;
  time_to_live: number | string;
  max_retries: number | string;
  retry_sleep_duration: number | string;
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

export const ENDPOINTTYPE = {
  HTTP: 'HTTP',
  AMQP: 'AMQP',
  Kafka: 'KAFKA'
};
export const AMQPACKLEVEL = {
  Select: 'Select AMQP level',
  none: 'none',
  broker: 'broker',
  routable: 'routable'
};
export const KAFKAACKLEVEL = {
  Select: 'Select KAFKA level',
  none: 'none',
  broker: 'broker'
};

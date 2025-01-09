interface Destination {
  push_endpoint: string;
  push_endpoint_args: string;
  push_endpoint_topic: string;
  stored_secret: string;
  persistent: boolean;
  persistent_queue: string;
  time_to_live: number;
  max_retries: number;
  retry_sleep_duration: number;
}

export interface Topic {
  owner: string;
  name: string;
  arn: string;
  dest: Destination;
  opaqueData: string;
  policy: string | {};
  subscribed_buckets: any[];
}

export interface TopicDetails {
  owner: string;
  name: string;
  arn: string;
  dest: Destination;
  opaqueData: string;
  policy: string;
  subscribed_buckets: string[];
}

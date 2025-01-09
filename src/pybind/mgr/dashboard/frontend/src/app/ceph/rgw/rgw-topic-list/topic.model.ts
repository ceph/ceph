export interface TopicModel {
  owner: string;
  name: string;
  dest: {
    push_endpoint: string;
    push_endpoint_args: string;
    push_endpoint_topic: string;
    stored_secret: string;
    persistent: string;
    persistent_queue: string;
    time_to_live: string;
    max_retries: string;
    retry_sleep_duration: string;
  };
  arn: string;
  opaqueData: string;
  policy: string;
  subscribed_buckets: string[];
}

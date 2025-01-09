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

export interface TopicModel {
  owner: string;
  name: string;
  arn: string;
  dest: Destination;
  opaqueData: string;
  policy: string | {};
  subscribed_buckets: any[];
}

interface PolicyStatement {
  Sid: string;
  Effect: string;
  Principal: {
    AWS: string;
  };
  Action: string[];
  Resource: string;
}

// Interface for the 'dest' object
interface Dest {
  push_endpoint: string;
  push_endpoint_args: string;
  push_endpoint_topic: string;
  stored_secret: boolean;
  persistent: boolean;
  persistent_queue: string;
  time_to_live: string;
  max_retries: string;
  retry_sleep_duration: string;
}

// The main TopicDetailModel interface for selection
export interface TopicDetailModel {
  owner: string;
  name: string;
  arn: string;
  dest: Dest; // Use the 'Dest' interface
  opaqueData: string;
  policy: {
    Statement: PolicyStatement[]; // Use the 'PolicyStatement' interface
  };
  subscribed_buckets: string[];
}

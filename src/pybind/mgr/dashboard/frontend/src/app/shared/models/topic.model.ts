export interface Destination {
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

export interface Topic {
  owner: string;
  name: string;
  arn: string;
  dest: Destination;
  opaqueData: string;
  policy: string | {};
  key: string;
  subscribed_buckets: any[];
}

export interface TopicRequest {
  owner: string;
  name: string;
  push_endpoint: string;
  opaque_data: string;
  policy: {} | string;
  persistent?: string;
  time_to_live?: string;
  max_retries?: string;
  retry_sleep_duration?: string;
  verify_ssl?: boolean;
  cloud_events?: string;
  ca_location?: string;
  amqp_exchange?: string;
  ack_level?: string;
  use_ssl?: boolean;
  kafka_brokers?: string;
  mechanism?: string;
}

export const KAFKA_MECHANISM = {
  PLAIN: 'PLAIN',
  SCRAM256: 'SCRAM-SHA-256',
  SCRAM512: 'SCRAM-SHA-512'
};
export const END_POINT_TYPE = {
  HTTP: 'HTTP',
  AMQP: 'AMQP',
  Kafka: 'KAFKA'
};
export const AMQP_ACK_LEVEL = {
  none: 'none',
  broker: 'broker',
  routable: 'routable'
};
export const KAFKA_ACK_LEVEL = {
  none: 'none',
  broker: 'broker'
};
export enum URLPort {
  HTTP = '80',
  HTTPS = '443',
  AMQP = '5672',
  AMQPS = '5671',
  KAFKA = '9092',
  KAFKA_SSL = '9093'
}
export enum HostURLProtocol {
  http = 'http',
  https = 'https',
  amqp = 'amqp',
  amqps = 'amqps',
  kafka = 'kafka'
}
export enum Endpoint {
  HTTP = 'HTTP',
  AMQP = 'AMQP',
  AMQPS = 'AMQPS',
  KAFKA = 'KAFKA'
}

export enum UrlProtocol {
  HTTP = 'http:',
  HTTPS = 'https:',
  AMQP = 'amqp:',
  AMQPS = 'amqps:',
  KAFKA = 'kafka'
}

export const URL_FORMAT_PLACEHOLDERS = {
  http: 'http[s]://<fqdn>[:<port]...',
  amqp: 'amqp[s]://[<user>:<password>@]<fqdn>[:<port>][/<vhost>]...',
  kafka: 'kafka://[<user>:<password>@]<fqdn>[:<port>]...'
};

import { ComboBoxItem } from "./combo-box.model";

interface Destination {
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
  subscribed_buckets: any[];
}

export interface CreateTopic {
  owner: string;
  name: string;
  push_endpoint: string;
  opaque_data: string;
  persistent?: string;
  time_to_live?: string;
  max_retries?: string;
  retry_sleep_duration?: string;
  policy: {} | string;
  verify_ssl?: boolean;
  cloud_events?: string;
  ca_location?: string;
  amqp_exchange?: string;
  amqp_ack_level?: string;
  use_ssl?: boolean;
  kafka_ack_level?: string;
  kafka_brokers?: string;
  mechanism?: string;
}

export const KAFKA_MECHANISM = {
  PLAIN: 'PLAIN',
  SCRAM256: 'SCRAM-SHA-256',
  SCRAM512: 'SCRAM-SHA-512',
  GSSAPI: 'GSSAPI',
  OAUTHBEARER: 'OAUTHBEARER'
};
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
export enum URLPort {
  HTTP = '80',
  HTTPS = '443',
  AMQP = '5672',
  AMQPS = '5671',
  KAFKA = '9092'
}
export enum HostURLProtocol {
  http = 'http',
  https = 'https',
  amqp = 'amqp',
  amqps = 'amqps',
  kafka = 'kafka',
  HTTP = 'HTTP',
  AMQP = 'AMQP',
  AMQPS = 'AMQPS',
  KAFKA = 'KAFKA'
}
export const EVENT_OPTIONS: ComboBoxItem[] = [
  { content: 's3:ObjectCreated:*', name: 's3:ObjectCreated:*' },
  { content: 's3:ObjectRemoved:*', name: 's3:ObjectRemoved:*' },
  { content: 's3:ObjectRestore:*', name: 's3:ObjectRestore:*' },
  { content: 's3:ObjectTagging:*', name: 's3:ObjectTagging:*' },
  { content: 's3:ObjectCreated:Put', name: 's3:ObjectCreated:Put' },
  { content: 's3:ObjectCreated:Post', name: 's3:ObjectCreated:Post' },
  { content: 's3:ObjectCreated:Copy', name: 's3:ObjectCreated:Copy', selected: true },
  { content: 's3:ObjectCreated:CompleteMultipartUpload', name: 's3:ObjectCreated:CompleteMultipartUpload' },
  { content: 's3:ObjectRemoved:Delete', name: 's3:ObjectRemoved:Delete' }
];

export enum S3KEYFILTERVALUE {
  select = 'Select S3 key filter',
  prefix = 'prefix',
  suffix = 'suffix',
  regex = 'regex',
}

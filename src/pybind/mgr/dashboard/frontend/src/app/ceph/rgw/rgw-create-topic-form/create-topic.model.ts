export interface CreateTopicModel {
  owner: string;
  name: string;
  push_endpoint: string;
  OpaqueData: string;
  persistent?: string;
  time_to_live?: string;
  max_retries?: string;
  retry_sleep_duration?: string;
  policy: {} | string;
  verify_ssl?: boolean;
  cloud_events?: string;
  ca_location: string;
  amqp_exchange?: string;
  amqp_ack_level?: string;
  use_ssl?: boolean;
  kafka_ack_level?: string;
  kafka_brokers?: string;
  mechanism?: string;
}

export const KAFKAMECHANISM = {
  PLAIN: 'PLAIN',
  SCRAM256: 'SCRAM-SHA-256',
  SCRAM512: 'SCRAM-SHA-512',
  GSSAPI: 'GSSAPI',
  OAUTHBEARER: 'OAUTHBEARER'
};

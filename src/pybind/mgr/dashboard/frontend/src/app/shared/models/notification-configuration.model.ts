import { ComboBoxItem } from './combo-box.model';

export interface NotificationConfig {
  NotificationConfiguration: NotificationConfiguration;
}

export interface NotificationConfiguration {
  TopicConfiguration: TopicConfiguration[];
}

export interface TopicConfiguration {
  Id: string;
  Topic: string;
  Event: string[];
  Filter?: Filter;
}

export interface Filter {
  S3Key: Key;
  S3Metadata: Metadata;
  S3Tags: Tags;
}

export interface Key {
  FilterRules: FilterRules[];
}
export interface Metadata {
  FilterRules: FilterRules[];
}
export interface Tags {
  FilterRules: FilterRules[];
}
export interface FilterRules {
  Name: string;
  Value: string;
}

export const events: ComboBoxItem[] = [
  { content: 's3:ObjectCreated:*', name: 's3:ObjectCreated:*' },
  { content: 's3:ObjectCreated:Put', name: 's3:ObjectCreated:Put' },
  { content: 's3:ObjectCreated:Copy', name: 's3:ObjectCreated:Copy' },
  {
    content: 's3:ObjectCreated:CompleteMultipartUpload',
    name: 's3:ObjectCreated:CompleteMultipartUpload'
  },
  { content: 's3:ObjectRemoved:*', name: 's3:ObjectRemoved:*' },
  { content: 's3:ObjectRemoved:Delete', name: 's3:ObjectRemoved:Delete' },
  { content: 's3:ObjectRemoved:DeleteMarkerCreated', name: 's3:ObjectRemoved:DeleteMarkerCreated' }
];

export enum s3KeyFilter {
  SELECT = '-- Select key filter type --',
  PREFIX = 'prefix',
  SUFFIX = 'suffix',
  REGX = 'regex'
}

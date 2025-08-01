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
  REGEX = 'regex'
}

export const s3KeyFilterTexts = {
  namePlaceholder: $localize`e.g. images/`,
  valuePlaceholder: $localize`e.g. .jpg`,
  nameHelper: $localize`Choose a filter type (prefix or suffix) to specify which object keys trigger the notification`,
  valueHelper: $localize`Enter the prefix (e.g. images/) or suffix (e.g. .jpg) value for the S3 key filter`
};

export const s3MetadataFilterTexts = {
  namePlaceholder: $localize`x-amz-meta-xxx...`,
  valuePlaceholder: $localize`e.g. my-custom-value`,
  nameHelper: $localize`Enter a metadata key name to identify the custom information`,
  valueHelper: $localize`Enter the metadata value that corresponds to the key`
};

export const s3TagsFilterTexts = {
  namePlaceholder: $localize`e.g. backup-status`,
  valuePlaceholder: $localize`e.g. completed`,
  nameHelper: $localize`Enter a tag key to categorize the S3 objects`,
  valueHelper: $localize`Enter the tag value that corresponds to the key`
};

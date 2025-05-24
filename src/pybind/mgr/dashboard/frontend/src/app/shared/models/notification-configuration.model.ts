export interface NotificationConfig {
  NotificationConfiguration: NotificationConfiguration;
}

export interface NotificationConfiguration {
  TopicConfiguration: TopicConfiguration[];
}

export interface TopicConfiguration {
  ID: string;
  Topic: string;
  Event: string[];
  Filter?: Filter;
}

export interface Filter {
  Key: Key;
  Metadata: Metadata;
  Tags: Tags;
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

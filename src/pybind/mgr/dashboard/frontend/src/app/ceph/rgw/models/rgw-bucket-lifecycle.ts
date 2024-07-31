export interface Lifecycle {
  LifecycleConfiguration: LifecycleConfiguration;
}

export interface LifecycleConfiguration {
  Rule: Rule[];
}

export interface Rule {
  ID: string;
  Status: string;
  Transition: Transition;
  Prefix?: string;
  Filter?: Filter;
}

export interface Filter {
  And?: And;
  Prefix?: string;
  Tag?: Tag | Tag[];
}

export interface And {
  Prefix: string;
  Tag: Tag | Tag[];
}

export interface Tag {
  Key: string;
  Value: string;
}

export interface Transition {
  Days: string;
  StorageClass: string;
}

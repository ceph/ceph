import { CephServicePlacement } from '~/app/shared/models/service.interface';

export interface SMBCluster {
  cluster_id: string;
  auth_mode: AuthMode;
  intent: string;
  domain_settings?: DomainSettings;
  user_group_settings?: string[];
  custom_dns?: string[];
  placement?: CephServicePlacement;
  clustering?: string;
  public_addrs?: PublicAddress;
}

export interface DomainSettings {
  realm?: string;
  join_sources_ref?: string[];
}

export interface PublicAddress {
  address: string;
  destination: string;
}

export interface AuthMode {
  user: 'User';
  activeDirectory: 'active-directory';
}

export interface JoinAuth {
  resource_type: string;
  auth_id: string;
  intent: 'present' | 'removed';
  auth: Auth;
  linked_to_cluster?: string;
}

export interface UsersGroups {
  resource_type: string;
  users_groups_id: string;
  intent: 'present' | 'removed';
  values: Value;
  linked_to_cluster?: string;
}

interface Auth {
  username: string;
  password: string;
}

interface User {
  name: string;
  password: string;
}

interface Group {
  name: string;
}

interface Value {
  users: User[];
  groups: Group[];
}

import { CephServicePlacement } from '~/app/shared/models/service.interface';

export interface SMBCluster {
  resource_type: typeof CLUSTER_RESOURCE;
  cluster_id: string;
  auth_mode: typeof AUTHMODE[keyof typeof AUTHMODE];
  domain_settings?: DomainSettings;
  user_group_settings?: JoinSource[];
  custom_dns?: string[];
  placement?: CephServicePlacement;
  clustering?: Clustering;
  public_addrs?: PublicAddress[];
  count?: number;
}

export interface ClusterRequestModel {
  cluster_resource: SMBCluster;
}

export interface ShareRequestModel {
  share_resource: SMBShare;
}

interface SMBCephfs {
  volume: string;
  path: string;
  subvolumegroup?: string;
  subvolume?: string;
  provider?: string;
}

interface SMBShareLoginControl {
  name: string;
  access: 'read' | 'read-write' | 'none' | 'admin';
  category?: 'user' | 'group';
}

export interface Filesystem {
  id: string;
  name: string;
}

export interface DomainSettings {
  realm?: string;
  join_sources?: JoinSource[];
}

export interface JoinSource {
  sourceType: string;
  ref: string;
}

export interface PublicAddress {
  address: string;
  destination?: string;
}

export const CLUSTERING = {
  Default: 'default',
  Always: 'always',
  Never: 'never'
};

export const RESOURCE = {
  ClusterResource: 'cluster_resource',
  Resource: 'resource'
};

export const AUTHMODE = {
  User: 'user',
  activeDirectory: 'active-directory'
};

export const PLACEMENT = {
  host: 'hosts',
  label: 'label'
};

export interface SMBShare {
  resource_type: string;
  cluster_id: string;
  share_id: string;
  cephfs: SMBCephfs;
  intent?: string;
  name?: string;
  readonly?: boolean;
  browseable?: boolean;
  restrict_access?: boolean;
  login_control?: SMBShareLoginControl;
}

interface SMBCephfs {
  volume: string;
  path: string;
  subvolumegroup?: string;
  subvolume?: string;
  provider?: string;
}

interface SMBShareLoginControl {
  name: string;
  access: 'read' | 'read-write' | 'none' | 'admin';
  category?: 'user' | 'group';
}

export interface SMBJoinAuth {
  resource_type: string;
  auth_id: string;
  auth: Auth;
  linked_to_cluster?: string;
}

export interface SMBUsersGroups {
  resource_type: string;
  users_groups_id: string;
  values: Value;
  linked_to_cluster?: string;
}

interface Auth {
  username: string;
  password: string;
}

export interface User {
  name: string;
  password: string;
}

export interface Group {
  name: string;
}

interface Value {
  users: User[];
  groups: Group[];
}

export type SMBResource = SMBCluster | SMBShare | SMBJoinAuth | SMBUsersGroups;

export const CLUSTER_RESOURCE = 'ceph.smb.cluster' as const;
export const SHARE_RESOURCE = 'ceph.smb.share' as const;
export const JOIN_AUTH_RESOURCE = 'ceph.smb.join.auth' as const;
export const USERSGROUPS_RESOURCE = 'ceph.smb.usersgroups' as const;

export const PROVIDER = 'samba-vfs';

type Clustering = 'default' | 'never' | 'always';

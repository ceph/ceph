import { CephServicePlacement } from '~/app/shared/models/service.interface';

export interface SMBCluster {
  resource_type: string;
  cluster_id: string;
  auth_mode: typeof AUTHMODE[keyof typeof AUTHMODE];
  domain_settings?: DomainSettings;
  user_group_settings?: JoinSource[];
  custom_dns?: string[];
  placement?: CephServicePlacement;
  clustering?: typeof CLUSTERING;
  public_addrs?: PublicAddress;
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
export interface PublicAddress {
  address: string;
  destination: string;
}
export interface JoinSource {
  source_type: string;
  ref: string;
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
  intent: Intent;
  auth: Auth;
  linked_to_cluster?: string;
}

export interface SMBUsersGroups {
  resource_type: string;
  users_groups_id: string;
  intent: Intent;
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

type Intent = 'present' | 'removed';

export const CLUSTER_RESOURCE = 'ceph.smb.cluster';

export const SHARE_RESOURCE = 'ceph.smb.share';

export const PROVIDER = 'samba-vfs';

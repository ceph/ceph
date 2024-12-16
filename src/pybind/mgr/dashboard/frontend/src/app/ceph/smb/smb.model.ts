import { CephServicePlacement } from '~/app/shared/models/service.interface';

export interface SMBCluster {
  //resource_type: typeof RESOURCE_TYPE;
  cluster_id: string;
  auth_mode: typeof AUTHMODE;
  domain_settings?: DomainSettings;
  user_group_settings?: JoinSource[];
  custom_dns?: string[];
  placement?: CephServicePlacement;
  clustering?: typeof CLUSTERING;
  public_addrs?: PublicAddress;
}

export interface RequestModel {
  cluster_resource: SMBCluster;
}

export interface DomainSettings {
  realm?: string;
  join_sources?: JoinSource[];
}

export interface JoinSource {
  source_type: string;
  ref: string;
}

export interface PublicAddress {
  address: string;
  destination: string;
}

export interface SMBShare {
  cluster_id: string;
  share_id: string;
  intent: string;
  name?: string;
  readonly?: boolean;
  browseable?: boolean;
  cephfs: SMBCephfs;
  restrict_access?: boolean;
  login_control?: SMBShareLoginControl;
}

export interface SMBShare {
  cluster_id: string;
  share_id: string;
  intent: string;
  name?: string;
  readonly?: boolean;
  browseable?: boolean;
  cephfs: SMBCephfs;
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
  category?: 'user' | 'group';
  access: 'read' | 'read-write' | 'none' | 'admin';
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

export const RESOURCE_TYPE = {
  cluster: 'ceph.smb.cluster',
  share: 'ceph.smb.share'
};

export const PROVIDER = 'samba-vfs';

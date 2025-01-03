import { CephServicePlacement } from '~/app/shared/models/service.interface';

export interface SMBCluster {
  resource_type: string;
  cluster_id: string;
  auth_mode: typeof AuthMode;
  domain_settings?: DomainSettings;
  user_group_settings?: DomainSettings['join_sources'][];
  custom_dns?: string[];
  placement?: CephServicePlacement;
  clustering?: typeof Clustering;
  public_addrs?: PublicAddress;
}

export interface DomainSettings {
  realm?: string;
  join_sources?: string[];
}

export interface PublicAddress {
  address: string;
  destination: string;
}

export const Clustering = {
  Default: 'default',
  Always: 'always',
  Never: 'never'
}

export const Resources = {
  ClusterResource: 'cluster_resource',
  Resource: 'resource'
}

export const AuthMode = {
  User: 'user',
  activeDirectory: 'active-directory'
}


export const Resource_Type = 'ceph.smb.cluster';

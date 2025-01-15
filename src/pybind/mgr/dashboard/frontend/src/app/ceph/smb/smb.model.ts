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

export interface SMBResult {
  resource: SMBCluster | SMBShare;
  state: string;
  success: boolean;
}

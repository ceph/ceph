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

export interface CephServiceStatus {
  container_image_id: string;
  container_image_name: string;
  size: number;
  running: number;
  last_refresh: Date;
  created: Date;
}

// This will become handy when creating arbitrary services
export interface CephServiceSpec {
  service_name: string;
  service_type: string;
  service_id: string;
  unmanaged: boolean;
  status: CephServiceStatus;
  spec: CephServiceAdditionalSpec;
  placement: CephServicePlacement;
}

export interface CephServiceAdditionalSpec {
  backend_service: string;
  api_user: string;
  api_password: string;
  api_port: number;
  api_secure: boolean;
  rgw_frontend_port: number;
  trusted_ip_list: string[];
  virtual_ip: string;
  frontend_port: number;
  monitor_port: number;
  virtual_interface_networks: string[];
  pool: string;
  group: string;
  root_ca_cert: string;
  client_cert: string;
  client_key: string;
  server_cert: string;
  server_key: string;
  rgw_frontend_ssl_certificate: string;
  ssl: boolean;
  ssl_cert: string;
  ssl_certificate: string;
  ssl_key: string;
  ssl_certificate_key: string;
  ssl_protocols: string[];
  ssl_ciphers: string[];
  port: number;
  initial_admin_password: string;
  rgw_realm: string;
  rgw_zonegroup: string;
  rgw_zone: string;
  cluster_id: string;
  features: string[];
  config_uri: string;
  custom_dns: string[];
  join_sources: string[];
  include_ceph_users: string[];
  https_address: string;
  provider_display_name: string;
  client_id: string;
  client_secret: string;
  oidc_issuer_url: string;
  enable_auth: boolean;
}

export interface CephServicePlacement {
  count?: number;
  placement?: string;
  hosts?: string[];
  label?: string | string[];
}

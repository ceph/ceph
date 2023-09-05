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
  rgw_frontend_ssl_certificate: string;
  ssl: boolean;
  ssl_cert: string;
  ssl_key: string;
  port: number;
  initial_admin_password: string;
  rgw_realm: string;
  rgw_zonegroup: string;
  rgw_zone: string;
}

export interface CephServicePlacement {
  count: number;
  placement: string;
  hosts: string[];
  label: string;
}

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
}

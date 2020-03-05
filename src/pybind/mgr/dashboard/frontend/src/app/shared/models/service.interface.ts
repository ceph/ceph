export interface CephService {
  container_image_id: string;
  container_image_name: string;
  service_name: string;
  size: number;
  running: number;
  last_refresh: Date;
}

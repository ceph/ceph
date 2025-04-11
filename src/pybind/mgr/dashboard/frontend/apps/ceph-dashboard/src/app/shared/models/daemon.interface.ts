export interface Daemon {
  nodename: string;
  container_id: string;
  container_image_id: string;
  container_image_name: string;
  daemon_id: string;
  daemon_type: string;
  daemon_name: string;
  hostname: string;
  version: string;
  status: number;
  status_desc: string;
  last_refresh: Date;
}

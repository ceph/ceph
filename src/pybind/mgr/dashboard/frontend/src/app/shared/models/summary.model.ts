import { ExecutingTask } from './executing-task';
import { FinishedTask } from './finished-task';

export class Summary {
  executing_tasks?: ExecutingTask[];
  filesystems?: any[];
  finished_tasks?: FinishedTask[];
  have_mon_connection?: boolean;
  health_status?: string;
  mgr_host?: string;
  mgr_id?: string;
  rbd_mirroring?: any;
  rbd_pools?: any[];
  version?: string;
}

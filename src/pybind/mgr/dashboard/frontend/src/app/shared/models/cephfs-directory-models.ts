import { TreeStatus } from '@swimlane/ngx-datatable';

export class CephfsSnapshot {
  name: string;
  path: string;
  created: string;
}

export class CephfsQuotas {
  max_bytes?: number;
  max_files?: number;
}

export class CephfsDir {
  name: string;
  path: string;
  quotas: CephfsQuotas;
  snapshots: CephfsSnapshot[];
  parent: string;
  treeStatus?: TreeStatus; // Needed for table tree view
}

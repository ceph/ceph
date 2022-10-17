import { SortPropDir } from '@swimlane/ngx-datatable';

import { CdTableColumn } from './cd-table-column';

export interface CdUserConfig {
  limit?: number;
  offset?: number;
  search?: string;
  sorts?: SortPropDir[];
  columns?: CdTableColumn[];
}

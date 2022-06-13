import { SortPropDir } from '@swimlane/ngx-datatable';

import { CdTableColumn } from './cd-table-column';

export interface CdUserConfig {
  limit?: number;
  offset?: number;
  sorts?: SortPropDir[];
  columns?: CdTableColumn[];
}

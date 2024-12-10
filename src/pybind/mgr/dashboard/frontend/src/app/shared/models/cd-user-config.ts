import { CdTableColumn } from './cd-table-column';
import { CdSortPropDir } from './cd-sort-prop-dir';

export interface CdUserConfig {
  limit?: number;
  offset?: number;
  search?: string;
  sorts?: CdSortPropDir[];
  columns?: CdTableColumn[];
}

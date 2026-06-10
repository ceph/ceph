import { CdTableColumn } from './cd-table-column';

export interface CdTableColumnFilter {
  column: CdTableColumn;
  options: CdTableColumnFilterOption[]; // possible options of a filter
  value?: CdTableColumnFilterOption; // selected option
}

export interface CdTableColumnStagedFilter {
  [filterName: string]: CdTableColumnFilterOption;
}

export interface CdTableColumnSelectedFilter {
  [filterName: string]: string | undefined;
}

export interface CdTableColumnFilterOption {
  raw: string;
  formatted: string;
}

export interface CdTableCustomColumnFilter {
  id: number;
  key: string;
  value: string;
}

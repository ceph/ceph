import { CdTableColumn } from './cd-table-column';

export interface CdTableColumnFilter {
  column: CdTableColumn;
  options: { raw: string; formatted: string }[]; // possible options of a filter
  value?: { raw: string; formatted: string }; // selected option
}

export type CdTableColumnFilterOption = { raw: string; formatted: string };

export interface CdTableActiveColumnFilter {
  isCustom: boolean;
  id: string;
  name: string;
  value: string;
  original: any;
}

export type CdTableColumnSelectedFilter = Record<string, any>;

export type CdTableColumnStagedFilter = Record<string, any>;

export interface CdTableCustomColumnFilter {
  id: number;
  key: string;
  value: string;
}

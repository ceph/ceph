import { CdTableColumn } from './cd-table-column';

export interface CdTableColumnFilter {
  column: CdTableColumn;
  options: { raw: string; formatted: string }[]; // possible options of a filter
  value?: { raw: string; formatted: string }; // selected option
}

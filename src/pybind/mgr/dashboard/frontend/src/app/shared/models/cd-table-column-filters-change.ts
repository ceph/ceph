import { TableColumnProp } from '@swimlane/ngx-datatable';

export interface CdTableColumnFiltersChange {
  /**
   * Applied filters.
   */
  filters: {
    name: string;
    prop: TableColumnProp;
    value: { raw: string; formatted: string };
  }[];

  /**
   * Filtered data.
   */
  data: any[];

  /**
   * Filtered out data.
   */
  dataOut: any[];
}

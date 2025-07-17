export interface CdTableColumnFiltersChange {
  /**
   * Applied filters.
   */
  filters: {
    name: string;
    prop: string | number;
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

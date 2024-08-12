import { TableColumn, TableColumnProp } from '@swimlane/ngx-datatable';

import { CellTemplate } from '../enum/cell-template.enum';

export interface CdTableColumn extends TableColumn {
  cellTransformation?: CellTemplate;
  isHidden?: boolean;
  prop: TableColumnProp; // Enforces properties to get sortable columns
  customTemplateConfig?: any; // Custom configuration used by cell templates.

  /**
   * Add a filter for the column if true.
   *
   * By default, options for the filter are deduced from values of the column.
   */
  filterable?: boolean;

  /**
   * Use these options for filter rather than deducing from values of the column.
   *
   * If there is a pipe function associated with the column, pipe function is applied
   * to the options before displaying them.
   */
  filterOptions?: any[];

  /**
   * Default applied option, should be value in filterOptions.
   */
  filterInitValue?: any;

  /**
   * Specify a custom function for filtering.
   *
   * By default, the filter compares if values are string-equal with options. Specify
   * a customize function if that's not desired. Return true to include a row.
   */
  filterPredicate?: (row: any, value: any) => boolean;

  /**
   * Hides a column from the 'toggle columns' drop down checkboxes
   */
  isInvisible?: boolean;
}

import { CellTemplate } from '../enum/cell-template.enum';
import { TableHeaderItem } from 'carbon-components-angular';
import { PipeTransform } from '@angular/core';

export interface CdTableColumn extends Partial<TableHeaderItem> {
  cellTransformation?: CellTemplate;

  isHidden?: boolean;

  prop?: string | number; // Enforces properties to get sortable columns

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

  name?: string;

  /**
   * Determines if column is checkbox
   *
   * @memberOf TableColumn
   */
  checkboxable?: boolean;
  /**
   * Determines if the column is frozen to the left
   *
   * @memberOf TableColumn
   */
  frozenLeft?: boolean;
  /**
   * Determines if the column is frozen to the right
   *
   * @memberOf TableColumn
   */
  frozenRight?: boolean;
  /**
   * The grow factor relative to other columns. Same as the flex-grow
   * API from http =//www.w3.org/TR/css3-flexbox/. Basically;
   * take any available extra width and distribute it proportionally
   * according to all columns' flexGrow values.
   *
   * @memberOf TableColumn
   */
  flexGrow?: number;
  /**
   * Min width of the column
   *
   * @memberOf TableColumn
   */
  minWidth?: number;
  /**
   * Max width of the column
   *
   * @memberOf TableColumn
   */
  maxWidth?: number;
  /**
   * The default width of the column, in pixels
   *
   * @memberOf TableColumn
   */
  width?: number;
  /**
   * Can the column be resized
   *
   * @memberOf TableColumn
   */
  resizeable?: boolean;
  /**
   * Custom sort comparator
   *
   * @memberOf TableColumn
   */
  comparator?: any;
  /**
   * Custom pipe transforms
   *
   * @memberOf TableColumn
   */
  pipe?: PipeTransform;
  /**
   * Can the column be sorted
   *
   * @memberOf TableColumn
   */
  sortable?: boolean;
  /**
   * Can the column be re-arranged by dragging
   *
   * @memberOf TableColumn
   */
  draggable?: boolean;
  /**
   * Whether the column can automatically resize to fill space in the table.
   *
   * @memberOf TableColumn
   */
  canAutoResize?: boolean;

  /**
   * Cell template ref
   *
   * @memberOf TableColumn
   */
  cellTemplate?: any;
  /**
   * Header template ref
   *
   * @memberOf TableColumn
   */
  headerTemplate?: any;
  /**
   * Tree toggle template ref
   *
   * @memberOf TableColumn
   */
  treeToggleTemplate?: any;
  /**
   * CSS Classes for the cell
   *
   *
   * @memberOf TableColumn
   */
  cellClass?: string | ((data: any) => string | any);
  /**
   * CSS classes for the header
   *
   *
   * @memberOf TableColumn
   */
  headerClass?: string | ((data: any) => string | any);
  /**
   * Header checkbox enabled
   *
   * @memberOf TableColumn
   */
  headerCheckboxable?: boolean;
  /**
   * Is tree displayed on this column
   *
   * @memberOf TableColumn
   */
  isTreeColumn?: boolean;
  /**
   * Width of the tree level indent
   *
   * @memberOf TableColumn
   */
  treeLevelIndent?: number;
  /**
   * Summary function
   *
   * @memberOf TableColumn
   */
  summaryFunc?: (cells: any[]) => any;
  /**
   * Summary cell template ref
   *
   * @memberOf TableColumn
   */
  summaryTemplate?: any;
}

import {
  AfterContentChecked,
  ChangeDetectionStrategy,
  ChangeDetectorRef,
  Component,
  EventEmitter,
  Input,
  NgZone,
  OnChanges,
  OnDestroy,
  OnInit,
  Output,
  PipeTransform,
  TemplateRef,
  ViewChild
} from '@angular/core';

import {
  DatatableComponent,
  getterForProp,
  SortDirection,
  SortPropDir,
  TableColumnProp
} from '@swimlane/ngx-datatable';
import * as _ from 'lodash';
import { Observable, Subject, Subscription, timer as observableTimer } from 'rxjs';

import { Icons } from '../../../shared/enum/icons.enum';
import { CellTemplate } from '../../enum/cell-template.enum';
import { CdTableColumn } from '../../models/cd-table-column';
import { CdTableColumnFilter } from '../../models/cd-table-column-filter';
import { CdTableColumnFiltersChange } from '../../models/cd-table-column-filters-change';
import { CdTableFetchDataContext } from '../../models/cd-table-fetch-data-context';
import { CdTableSelection } from '../../models/cd-table-selection';
import { CdUserConfig } from '../../models/cd-user-config';

@Component({
  selector: 'cd-table',
  templateUrl: './table.component.html',
  styleUrls: ['./table.component.scss'],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class TableComponent implements AfterContentChecked, OnInit, OnChanges, OnDestroy {
  @ViewChild(DatatableComponent, { static: true })
  table: DatatableComponent;
  @ViewChild('tableCellBoldTpl', { static: true })
  tableCellBoldTpl: TemplateRef<any>;
  @ViewChild('sparklineTpl', { static: true })
  sparklineTpl: TemplateRef<any>;
  @ViewChild('routerLinkTpl', { static: true })
  routerLinkTpl: TemplateRef<any>;
  @ViewChild('checkIconTpl', { static: true })
  checkIconTpl: TemplateRef<any>;
  @ViewChild('perSecondTpl', { static: true })
  perSecondTpl: TemplateRef<any>;
  @ViewChild('executingTpl', { static: true })
  executingTpl: TemplateRef<any>;
  @ViewChild('classAddingTpl', { static: true })
  classAddingTpl: TemplateRef<any>;
  @ViewChild('badgeTpl', { static: true })
  badgeTpl: TemplateRef<any>;
  @ViewChild('mapTpl', { static: true })
  mapTpl: TemplateRef<any>;
  @ViewChild('truncateTpl', { static: true })
  truncateTpl: TemplateRef<any>;
  @ViewChild('rowDetailsTpl', { static: true })
  rowDetailsTpl: TemplateRef<any>;

  // This is the array with the items to be shown.
  @Input()
  data: any[];
  // Each item -> { prop: 'attribute name', name: 'display name' }
  @Input()
  columns: CdTableColumn[];
  // Each item -> { prop: 'attribute name', dir: 'asc'||'desc'}
  @Input()
  sorts?: SortPropDir[];
  // Method used for setting column widths.
  @Input()
  columnMode? = 'flex';
  // Display only actions in header (make sure to disable toolHeader) and use ".only-table-actions"
  @Input()
  onlyActionHeader? = false;
  // Display the tool header, including reload button, pagination and search fields?
  @Input()
  toolHeader? = true;
  // Display search field inside tool header?
  @Input()
  searchField? = true;
  // Display the table header?
  @Input()
  header? = true;
  // Display the table footer?
  @Input()
  footer? = true;
  // Page size to show. Set to 0 to show unlimited number of rows.
  @Input()
  limit? = 10;
  // Has the row details?
  @Input()
  hasDetails = false;

  /**
   * Auto reload time in ms - per default every 5s
   * You can set it to 0, undefined or false to disable the auto reload feature in order to
   * trigger 'fetchData' if the reload button is clicked.
   * You can set it to a negative number to, on top of disabling the auto reload,
   * prevent triggering fetchData when initializing the table.
   */
  @Input()
  autoReload: any = 5000;

  // Which row property is unique for a row. If the identifier is not specified in any
  // column, then the property name of the first column is used. Defaults to 'id'.
  @Input()
  identifier = 'id';
  // If 'true', then the specified identifier is used anyway, although it is not specified
  // in any column. Defaults to 'false'.
  @Input()
  forceIdentifier = false;
  // Allows other components to specify which type of selection they want,
  // e.g. 'single' or 'multi'.
  @Input()
  selectionType: string = undefined;
  // By default selected item details will be updated on table refresh, if data has changed
  @Input()
  updateSelectionOnRefresh: 'always' | 'never' | 'onChange' = 'onChange';
  // By default expanded item details will be updated on table refresh, if data has changed
  @Input()
  updateExpandedOnRefresh: 'always' | 'never' | 'onChange' = 'onChange';

  @Input()
  autoSave = true;

  // Enable this in order to search through the JSON of any used object.
  @Input()
  searchableObjects = false;

  // Only needed to set if the classAddingTpl is used
  @Input()
  customCss?: { [css: string]: number | string | ((any: any) => boolean) };

  // Columns that aren't displayed but can be used as filters
  @Input()
  extraFilterableColumns: CdTableColumn[] = [];

  /**
   * Should be a function to update the input data if undefined nothing will be triggered
   *
   * Sometimes it's useful to only define fetchData once.
   * Example:
   * Usage of multiple tables with data which is updated by the same function
   * What happens:
   * The function is triggered through one table and all tables will update
   */
  @Output()
  fetchData = new EventEmitter();

  /**
   * This should be defined if you need access to the selection object.
   *
   * Each time the table selection changes, this will be triggered and
   * the new selection object will be sent.
   *
   * @memberof TableComponent
   */
  @Output()
  updateSelection = new EventEmitter();

  @Output()
  setExpandedRow = new EventEmitter();

  /**
   * This should be defined if you need access to the applied column filters.
   *
   * Each time the column filters changes, this will be triggered and
   * the column filters change event will be sent.
   *
   * @memberof TableComponent
   */
  @Output() columnFiltersChanged = new EventEmitter<CdTableColumnFiltersChange>();

  /**
   * Use this variable to access the selected row(s).
   */
  selection = new CdTableSelection();

  /**
   * Use this variable to access the expanded row
   */
  expanded: any = undefined;

  /**
   * To prevent making changes to the original columns list, that might change
   * how the table is renderer a second time, we now clone that list into a
   * local variable and only use the clone.
   */
  localColumns: CdTableColumn[];
  tableColumns: CdTableColumn[];
  icons = Icons;
  cellTemplates: {
    [key: string]: TemplateRef<any>;
  } = {};
  search = '';
  rows: any[] = [];
  loadingIndicator = true;
  loadingError = false;
  paginationClasses = {
    pagerLeftArrow: Icons.leftArrowDouble,
    pagerRightArrow: Icons.rightArrowDouble,
    pagerPrevious: Icons.leftArrow,
    pagerNext: Icons.rightArrow
  };
  userConfig: CdUserConfig = {};
  tableName: string;
  localStorage = window.localStorage;
  private saveSubscriber: Subscription;
  private reloadSubscriber: Subscription;
  private updating = false;

  // Internal variable to check if it is necessary to recalculate the
  // table columns after the browser window has been resized.
  private currentWidth: number;

  columnFilters: CdTableColumnFilter[] = [];
  selectedFilter: CdTableColumnFilter;
  get columnFiltered(): boolean {
    return _.some(this.columnFilters, (filter) => {
      return filter.value !== undefined;
    });
  }

  constructor(private ngZone: NgZone, private cdRef: ChangeDetectorRef) {}

  static prepareSearch(search: string) {
    search = search.toLowerCase().replace(/,/g, '');
    if (search.match(/['"][^'"]+['"]/)) {
      search = search.replace(/['"][^'"]+['"]/g, (match: string) => {
        return match.replace(/(['"])([^'"]+)(['"])/g, '$2').replace(/ /g, '+');
      });
    }
    return search.split(' ').filter((word) => word);
  }

  ngOnInit() {
    this.localColumns = _.clone(this.columns);

    // ngx-datatable triggers calculations each time mouse enters a row,
    // this will prevent that.
    this.table.element.addEventListener('mouseenter', (e) => e.stopPropagation());
    this._addTemplates();
    if (!this.sorts) {
      // Check whether the specified identifier exists.
      const exists = _.findIndex(this.localColumns, ['prop', this.identifier]) !== -1;
      // Auto-build the sorting configuration. If the specified identifier doesn't exist,
      // then use the property of the first column.
      this.sorts = this.createSortingDefinition(
        exists ? this.identifier : this.localColumns[0].prop + ''
      );
      // If the specified identifier doesn't exist and it is not forced to use it anyway,
      // then use the property of the first column.
      if (!exists && !this.forceIdentifier) {
        this.identifier = this.localColumns[0].prop + '';
      }
    }

    this.initUserConfig();
    this.localColumns.forEach((c) => {
      if (c.cellTransformation) {
        c.cellTemplate = this.cellTemplates[c.cellTransformation];
      }
      if (!c.flexGrow) {
        c.flexGrow = c.prop + '' === this.identifier ? 1 : 2;
      }
      if (!c.resizeable) {
        c.resizeable = false;
      }
    });

    this.initExpandCollapseColumn(); // If rows have details, add a column to expand or collapse the rows
    this.initCheckboxColumn();
    this.filterHiddenColumns();
    this.initColumnFilters();
    this.updateColumnFilterOptions();
    // Notify all subscribers to reset their current selection.
    this.updateSelection.emit(new CdTableSelection());
    // Load the data table content every N ms or at least once.
    // Force showing the loading indicator if there are subscribers to the fetchData
    // event. This is necessary because it has been set to False in useData() when
    // this method was triggered by ngOnChanges().
    if (this.fetchData.observers.length > 0) {
      this.loadingIndicator = true;
    }
    if (_.isInteger(this.autoReload) && this.autoReload > 0) {
      this.ngZone.runOutsideAngular(() => {
        this.reloadSubscriber = observableTimer(0, this.autoReload).subscribe(() => {
          this.ngZone.run(() => {
            return this.reloadData();
          });
        });
      });
    } else if (!this.autoReload) {
      this.reloadData();
    } else {
      this.useData();
    }
  }

  initUserConfig() {
    if (this.autoSave) {
      this.tableName = this._calculateUniqueTableName(this.localColumns);
      this._loadUserConfig();
      this._initUserConfigAutoSave();
    }
    if (!this.userConfig.limit) {
      this.userConfig.limit = this.limit;
    }
    if (!this.userConfig.sorts) {
      this.userConfig.sorts = this.sorts;
    }
    if (!this.userConfig.columns) {
      this.updateUserColumns();
    } else {
      this.localColumns.forEach((c, i) => {
        c.isHidden = this.userConfig.columns[i].isHidden;
      });
    }
  }

  _calculateUniqueTableName(columns: any[]) {
    const stringToNumber = (s: string) => {
      if (!_.isString(s)) {
        return 0;
      }
      let result = 0;
      for (let i = 0; i < s.length; i++) {
        result += s.charCodeAt(i) * i;
      }
      return result;
    };
    return columns
      .reduce(
        (result, value, index) =>
          (stringToNumber(value.prop) + stringToNumber(value.name)) * (index + 1) + result,
        0
      )
      .toString();
  }

  _loadUserConfig() {
    const loaded = this.localStorage.getItem(this.tableName);
    if (loaded) {
      this.userConfig = JSON.parse(loaded);
    }
  }

  _initUserConfigAutoSave() {
    const source: Observable<any> = new Observable(this._initUserConfigProxy.bind(this));
    this.saveSubscriber = source.subscribe(this._saveUserConfig.bind(this));
  }

  _initUserConfigProxy(observer: Subject<any>) {
    this.userConfig = new Proxy(this.userConfig, {
      set(config, prop: string, value) {
        config[prop] = value;
        observer.next(config);
        return true;
      }
    });
  }

  _saveUserConfig(config: any) {
    this.localStorage.setItem(this.tableName, JSON.stringify(config));
  }

  updateUserColumns() {
    this.userConfig.columns = this.localColumns.map((c) => ({
      prop: c.prop,
      name: c.name,
      isHidden: !!c.isHidden
    }));
  }

  /**
   * Add a column containing a checkbox if selectionType is 'multiClick'.
   */
  initCheckboxColumn() {
    if (this.selectionType === 'multiClick') {
      this.localColumns.unshift({
        prop: undefined,
        resizeable: false,
        sortable: false,
        draggable: false,
        checkboxable: true,
        canAutoResize: false,
        cellClass: 'cd-datatable-checkbox',
        width: 30
      });
    }
  }

  /**
   * Add a column to expand and collapse the table row if it 'hasDetails'
   */
  initExpandCollapseColumn() {
    if (this.hasDetails) {
      this.localColumns.unshift({
        prop: undefined,
        resizeable: false,
        sortable: false,
        draggable: false,
        isHidden: false,
        canAutoResize: false,
        cellClass: 'cd-datatable-expand-collapse',
        width: 40,
        cellTemplate: this.rowDetailsTpl
      });
    }
  }

  filterHiddenColumns() {
    this.tableColumns = this.localColumns.filter((c) => !c.isHidden);
  }

  initColumnFilters() {
    let filterableColumns = _.filter(this.localColumns, { filterable: true });
    filterableColumns = [...filterableColumns, ...this.extraFilterableColumns];
    this.columnFilters = filterableColumns.map((col: CdTableColumn) => {
      return {
        column: col,
        options: [],
        value: col.filterInitValue
          ? this.createColumnFilterOption(col.filterInitValue, col.pipe)
          : undefined
      };
    });
    this.selectedFilter = _.first(this.columnFilters);
  }

  private createColumnFilterOption(
    value: any,
    pipe?: PipeTransform
  ): { raw: string; formatted: string } {
    return {
      raw: _.toString(value),
      formatted: pipe ? pipe.transform(value) : _.toString(value)
    };
  }

  updateColumnFilterOptions() {
    // update all possible values in a column
    this.columnFilters.forEach((filter) => {
      let values: any[] = [];

      if (_.isUndefined(filter.column.filterOptions)) {
        // only allow types that can be easily converted into string
        const pre = _.filter(_.map(this.data, filter.column.prop), (v) => {
          return (_.isString(v) && v !== '') || _.isBoolean(v) || _.isFinite(v) || _.isDate(v);
        });
        values = _.sortedUniq(pre.sort());
      } else {
        values = filter.column.filterOptions;
      }

      const options = values.map((v) => this.createColumnFilterOption(v, filter.column.pipe));

      // In case a previous value is not available anymore
      if (filter.value && _.isUndefined(_.find(options, { raw: filter.value.raw }))) {
        filter.value = undefined;
      }

      filter.options = options;
    });
  }

  onSelectFilter(filter: CdTableColumnFilter) {
    this.selectedFilter = filter;
  }

  onChangeFilter(filter: CdTableColumnFilter, option?: { raw: string; formatted: string }) {
    filter.value = _.isEqual(filter.value, option) ? undefined : option;
    this.updateFilter();
  }

  doColumnFiltering() {
    const appliedFilters: CdTableColumnFiltersChange['filters'] = [];
    let data = [...this.data];
    let dataOut: any[] = [];
    this.columnFilters.forEach((filter) => {
      if (filter.value === undefined) {
        return;
      }
      appliedFilters.push({
        name: filter.column.name,
        prop: filter.column.prop,
        value: filter.value
      });
      // Separate data to filtered and filtered-out parts.
      const parts = _.partition(data, (row) => {
        // Use getter from ngx-datatable to handle props like 'sys_api.size'
        const valueGetter = getterForProp(filter.column.prop);
        const value = valueGetter(row, filter.column.prop);
        if (_.isUndefined(filter.column.filterPredicate)) {
          // By default, test string equal
          return `${value}` === filter.value.raw;
        } else {
          // Use custom function to filter
          return filter.column.filterPredicate(row, filter.value.raw);
        }
      });
      data = parts[0];
      dataOut = [...dataOut, ...parts[1]];
    });

    this.columnFiltersChanged.emit({
      filters: appliedFilters,
      data: data,
      dataOut: dataOut
    });

    // Remove the selection if previously-selected rows are filtered out.
    _.forEach(this.selection.selected, (selectedItem) => {
      if (_.find(data, { [this.identifier]: selectedItem[this.identifier] }) === undefined) {
        this.selection = new CdTableSelection();
        this.onSelect(this.selection);
      }
    });
    return data;
  }

  ngOnDestroy() {
    if (this.reloadSubscriber) {
      this.reloadSubscriber.unsubscribe();
    }
    if (this.saveSubscriber) {
      this.saveSubscriber.unsubscribe();
    }
  }

  ngAfterContentChecked() {
    // If the data table is not visible, e.g. another tab is active, and the
    // browser window gets resized, the table and its columns won't get resized
    // automatically if the tab gets visible again.
    // https://github.com/swimlane/ngx-datatable/issues/193
    // https://github.com/swimlane/ngx-datatable/issues/193#issuecomment-329144543
    if (this.table && this.table.element.clientWidth !== this.currentWidth) {
      this.currentWidth = this.table.element.clientWidth;
      // Recalculate the sizes of the grid.
      this.table.recalculate();
      // Mark the datatable as changed, Angular's change-detection will
      // do the rest for us => the grid will be redrawn.
      // Note, the ChangeDetectorRef variable is private, so we need to
      // use this workaround to access it and make TypeScript happy.
      const cdRef = _.get(this.table, 'cd');
      cdRef.markForCheck();
    }
  }

  _addTemplates() {
    this.cellTemplates.bold = this.tableCellBoldTpl;
    this.cellTemplates.checkIcon = this.checkIconTpl;
    this.cellTemplates.sparkline = this.sparklineTpl;
    this.cellTemplates.routerLink = this.routerLinkTpl;
    this.cellTemplates.perSecond = this.perSecondTpl;
    this.cellTemplates.executing = this.executingTpl;
    this.cellTemplates.classAdding = this.classAddingTpl;
    this.cellTemplates.badge = this.badgeTpl;
    this.cellTemplates.map = this.mapTpl;
    this.cellTemplates.truncate = this.truncateTpl;
  }

  useCustomClass(value: any): string {
    if (!this.customCss) {
      throw new Error('Custom classes are not set!');
    }
    const classes = Object.keys(this.customCss);
    const css = Object.values(this.customCss)
      .map((v, i) => ((_.isFunction(v) && v(value)) || v === value) && classes[i])
      .filter((x) => x)
      .join(' ');
    return _.isEmpty(css) ? undefined : css;
  }

  ngOnChanges() {
    this.useData();
  }

  setLimit(e: any) {
    const value = parseInt(e.target.value, 10);
    if (value > 0) {
      this.userConfig.limit = value;
    }
  }

  reloadData() {
    if (!this.updating) {
      this.loadingError = false;
      const context = new CdTableFetchDataContext(() => {
        // Do we have to display the error panel?
        this.loadingError = context.errorConfig.displayError;
        // Force data table to show no data?
        if (context.errorConfig.resetData) {
          this.data = [];
        }
        // Stop the loading indicator and reset the data table
        // to the correct state.
        this.useData();
      });
      this.fetchData.emit(context);
      this.updating = true;
    }
  }

  refreshBtn() {
    this.loadingIndicator = true;
    this.reloadData();
  }

  rowIdentity() {
    return (row: any) => {
      const id = row[this.identifier];
      if (_.isUndefined(id)) {
        throw new Error(`Wrong identifier "${this.identifier}" -> "${id}"`);
      }
      return id;
    };
  }

  useData() {
    if (!this.data) {
      return; // Wait for data
    }
    this.updateColumnFilterOptions();
    this.updateFilter();
    this.reset();
    this.updateSelected();
    this.updateExpanded();
  }

  /**
   * Reset the data table to correct state. This includes:
   * - Disable loading indicator
   * - Reset 'Updating' flag
   */
  reset() {
    this.loadingIndicator = false;
    this.updating = false;
  }

  /**
   * After updating the data, we have to update the selected items
   * because details may have changed,
   * or some selected items may have been removed.
   */
  updateSelected() {
    if (this.updateSelectionOnRefresh === 'never') {
      return;
    }
    const newSelected: any[] = [];
    this.selection.selected.forEach((selectedItem) => {
      for (const row of this.data) {
        if (selectedItem[this.identifier] === row[this.identifier]) {
          newSelected.push(row);
        }
      }
    });
    if (
      this.updateSelectionOnRefresh === 'onChange' &&
      _.isEqual(this.selection.selected, newSelected)
    ) {
      return;
    }
    this.selection.selected = newSelected;
    this.onSelect(this.selection);
  }

  updateExpanded() {
    if (_.isUndefined(this.expanded) || this.updateExpandedOnRefresh === 'never') {
      return;
    }

    const expandedId = this.expanded[this.identifier];
    const newExpanded = _.find(this.data, (row) => expandedId === row[this.identifier]);

    if (this.updateExpandedOnRefresh === 'onChange' && _.isEqual(this.expanded, newExpanded)) {
      return;
    }

    this.expanded = newExpanded;
    this.setExpandedRow.emit(newExpanded);
  }

  onSelect($event: any) {
    this.selection.selected = $event['selected'];
    this.updateSelection.emit(_.clone(this.selection));
  }

  toggleColumn(column: CdTableColumn) {
    const prop: TableColumnProp = column.prop;
    const hide = !column.isHidden;
    if (hide && this.tableColumns.length === 1) {
      column.isHidden = true;
      return;
    }
    _.find(this.localColumns, (c: CdTableColumn) => c.prop === prop).isHidden = hide;
    this.updateColumns();
  }

  updateColumns() {
    this.updateUserColumns();
    this.filterHiddenColumns();
    const sortProp = this.userConfig.sorts[0].prop;
    if (!_.find(this.tableColumns, (c: CdTableColumn) => c.prop === sortProp)) {
      this.userConfig.sorts = this.createSortingDefinition(this.tableColumns[0].prop);
    }
    this.table.recalculate();
    this.cdRef.detectChanges();
  }

  createSortingDefinition(prop: TableColumnProp): SortPropDir[] {
    return [
      {
        prop: prop,
        dir: SortDirection.asc
      }
    ];
  }

  changeSorting({ sorts }: any) {
    this.userConfig.sorts = sorts;
  }

  onClearSearch() {
    this.search = '';
    this.updateFilter();
  }

  onClearFilters() {
    this.columnFilters.forEach((filter) => {
      filter.value = undefined;
    });
    this.selectedFilter = _.first(this.columnFilters);
    this.updateFilter();
  }

  updateFilter() {
    let rows = this.columnFilters.length !== 0 ? this.doColumnFiltering() : this.data;

    if (this.search.length > 0 && rows) {
      const columns = this.localColumns.filter(
        (c) => c.cellTransformation !== CellTemplate.sparkline
      );
      // update the rows
      rows = this.subSearch(rows, TableComponent.prepareSearch(this.search), columns);
      // Whenever the filter changes, always go back to the first page
      this.table.offset = 0;
    }

    this.rows = rows;
  }

  subSearch(data: any[], currentSearch: string[], columns: CdTableColumn[]): any[] {
    if (currentSearch.length === 0 || data.length === 0) {
      return data;
    }
    const searchTerms: string[] = currentSearch.pop().replace(/\+/g, ' ').split(':');
    const columnsClone = [...columns];
    if (searchTerms.length === 2) {
      columns = columnsClone.filter((c) => c.name.toLowerCase().indexOf(searchTerms[0]) !== -1);
    }
    data = this.basicDataSearch(_.last(searchTerms), data, columns);
    // Checks if user searches for column but he is still typing
    return this.subSearch(data, currentSearch, columnsClone);
  }

  basicDataSearch(searchTerm: string, rows: any[], columns: CdTableColumn[]) {
    if (searchTerm.length === 0) {
      return rows;
    }
    return rows.filter((row) => {
      return (
        columns.filter((col) => {
          let cellValue: any = _.get(row, col.prop);

          if (!_.isUndefined(col.pipe)) {
            cellValue = col.pipe.transform(cellValue);
          }
          if (_.isUndefined(cellValue) || _.isNull(cellValue)) {
            return false;
          }

          if (_.isArray(cellValue)) {
            cellValue = cellValue.join(' ');
          } else if (_.isNumber(cellValue) || _.isBoolean(cellValue)) {
            cellValue = cellValue.toString();
          }

          if (_.isObjectLike(cellValue)) {
            if (this.searchableObjects) {
              cellValue = JSON.stringify(cellValue);
            } else {
              return false;
            }
          }

          return cellValue.toLowerCase().indexOf(searchTerm) !== -1;
        }).length > 0
      );
    });
  }

  getRowClass() {
    // Return the function used to populate a row's CSS classes.
    return () => {
      return {
        clickable: !_.isUndefined(this.selectionType)
      };
    };
  }

  toggleExpandRow(row: any, isExpanded: boolean) {
    if (!isExpanded) {
      // If current row isn't expanded, collapse others
      this.expanded = row;
      this.table.rowDetail.collapseAllRows();
      this.setExpandedRow.emit(row);
    } else {
      // If all rows are closed, emit undefined
      this.expanded = undefined;
      this.setExpandedRow.emit(undefined);
    }
    this.table.rowDetail.toggleExpandRow(row);
  }
}

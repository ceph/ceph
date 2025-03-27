import {
  AfterViewInit,
  ChangeDetectionStrategy,
  ChangeDetectorRef,
  Component,
  ContentChild,
  EventEmitter,
  Input,
  OnChanges,
  OnDestroy,
  OnInit,
  Output,
  PipeTransform,
  SimpleChanges,
  TemplateRef,
  ViewChild
} from '@angular/core';

import { TableHeaderItem, TableItem, TableModel, TableRowSize } from 'carbon-components-angular';
import _ from 'lodash';
import { BehaviorSubject, Observable, of, Subject, Subscription } from 'rxjs';

import { TableStatus } from '~/app/shared/classes/table-status';
import { CellTemplate } from '~/app/shared/enum/cell-template.enum';
import { Icons } from '~/app/shared/enum/icons.enum';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { CdTableColumnFilter } from '~/app/shared/models/cd-table-column-filter';
import { CdTableColumnFiltersChange } from '~/app/shared/models/cd-table-column-filters-change';
import { CdTableFetchDataContext } from '~/app/shared/models/cd-table-fetch-data-context';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { CdUserConfig } from '~/app/shared/models/cd-user-config';
import { TimerService } from '~/app/shared/services/timer.service';
import { TableActionsComponent } from '../table-actions/table-actions.component';
import { TableDetailDirective } from '../directives/table-detail.directive';
import { filter, map } from 'rxjs/operators';
import { CdSortDirection } from '../../enum/cd-sort-direction';
import { CdSortPropDir } from '../../models/cd-sort-prop-dir';

const TABLE_LIST_LIMIT = 10;
type TPaginationInput = { page: number; size: number; filteredData: any[] };
type TPaginationOutput = { start: number; end: number };

@Component({
  selector: 'cd-table',
  templateUrl: './table.component.html',
  styleUrls: ['./table.component.scss'],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class TableComponent implements AfterViewInit, OnInit, OnChanges, OnDestroy {
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
  @ViewChild('timeAgoTpl', { static: true })
  timeAgoTpl: TemplateRef<any>;
  @ViewChild('rowDetailsTpl', { static: true })
  rowDetailsTpl: TemplateRef<any>;
  @ViewChild('rowSelectionTpl', { static: true })
  rowSelectionTpl: TemplateRef<any>;
  @ViewChild('pathTpl', { static: true })
  pathTpl: TemplateRef<any>;
  @ViewChild('tooltipTpl', { static: true })
  tooltipTpl: TemplateRef<any>;
  @ViewChild('copyTpl', { static: true })
  copyTpl: TemplateRef<any>;
  @ViewChild('defaultValueTpl', { static: true })
  defaultValueTpl: TemplateRef<any>;
  @ViewChild('rowDetailTpl', { static: true })
  rowDetailTpl: TemplateRef<any>;
  @ViewChild('tableActionTpl', { static: true })
  tableActionTpl: TemplateRef<any>;

  @ContentChild(TableDetailDirective) rowDetail!: TableDetailDirective;
  @ContentChild(TableActionsComponent) tableActions!: TableActionsComponent;

  @Input()
  headerTitle: string;
  @Input()
  headerDescription: string;
  // This is the array with the items to be shown.
  @Input()
  data: any[];
  // Each item -> { prop: 'attribute name', name: 'display name' }
  @Input()
  columns: CdTableColumn[];
  // Each item -> { prop: 'attribute name', dir: 'asc'||'desc'}
  @Input()
  sorts?: CdSortPropDir[];
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
  limit? = TABLE_LIST_LIMIT;
  @Input()
  maxLimit? = 9999;
  // Has the row details?
  @Input()
  hasDetails = false;

  @Input()
  showInlineActions = true;

  /**
   * Auto reload time in ms - per default every 5s
   * You can set it to 0, undefined or false to disable the auto reload feature in order to
   * trigger 'fetchData' if the reload button is clicked.
   * You can set it to a negative number to, on top of disabling the auto reload,
   * prevent triggering fetchData when initializing the table.
   */
  @Input()
  autoReload = 5000;

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

  @Input()
  status = new TableStatus();

  // Support server-side pagination/sorting/etc.
  @Input()
  serverSide = false;

  @Input()
  size: TableRowSize = 'md';

  /*
  Only required when serverSide is enabled.
  It should be provided by the server via "X-Total-Count" HTTP Header
  */
  @Input()
  count = 0;

  /**
   * Use to change the colour layer you want to render the table at
   */
  @Input()
  layer: number;

  /**
   * Use to render table with a different theme than default one
   */
  @Input()
  theme: string;

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
  fetchData = new EventEmitter<CdTableFetchDataContext>();

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
  set expanded(value: any) {
    this._expanded = value;
    this.setExpandedRow.emit(value);
  }

  get expanded() {
    return this._expanded;
  }

  private _expanded: any = undefined;

  get sortable() {
    return !!this.userConfig?.sorts;
  }

  get noData() {
    return !this.rows?.length && !this.loadingIndicator;
  }

  get showSelectionColumn() {
    return this.selectionType === 'multiClick';
  }

  get enableSingleSelect() {
    return this.selectionType === 'single';
  }

  /**
   * Controls if all checkboxes are viewed as selected.
   */
  selectAllCheckbox = false;

  /**
   * Controls the indeterminate state of the header checkbox.
   */
  selectAllCheckboxSomeSelected = false;

  /**
   * To prevent making changes to the original columns list, that might change
   * how the table is renderer a second time, we now clone that list into a
   * local variable and only use the clone.
   */
  set localColumns(value: CdTableColumn[]) {
    this._localColumns = this.getTableColumnsWithNames(value);
  }

  get localColumns(): CdTableColumn[] {
    return this._localColumns;
  }

  private _localColumns: CdTableColumn[];

  model: TableModel = new TableModel();

  set tableColumns(value: CdTableColumn[]) {
    // In case a name is not provided set it to the prop name if present or an empty string
    const valuesWithNames = this.getTableColumnsWithNames(value);
    this._tableColumns = valuesWithNames;
    this._tableHeaders.next(valuesWithNames);
  }

  get tableColumns() {
    return this._tableColumns;
  }

  private _tableColumns: CdTableColumn[];

  get visibleColumns() {
    return this.localColumns?.filter?.((x) => !x.isHidden);
  }

  getTableColumnsWithNames(value: CdTableColumn[]): CdTableColumn[] {
    return value.map((col: CdTableColumn) =>
      col?.name ? col : { ...col, name: col?.prop ? this.deCamelCase(String(col?.prop)) : '' }
    );
  }

  deCamelCase(str: string): string {
    return str
      .replace(/([A-Z])/g, (match) => ` ${match}`)
      .replace(/^./, (match) => match.toUpperCase());
  }

  icons = Icons;
  cellTemplates: {
    [key: string]: TemplateRef<any>;
  } = {};
  search = '';

  set rows(value: any[]) {
    this._rows = value;
    this.doPagination({
      page: this.model.currentPage,
      size: this.model.pageLength,
      filteredData: value
    });
    this.model.totalDataLength = this.serverSide ? this.count : value?.length || 0;
  }

  get rows() {
    return this._rows;
  }

  private _rows: any[] = [];

  private _dataset = new BehaviorSubject<any[]>([]);

  private _tableHeaders = new BehaviorSubject<CdTableColumn[]>([]);

  private _subscriptions: Subscription = new Subscription();

  loadingIndicator = true;

  userConfig: CdUserConfig = {};
  tableName: string;
  localStorage = window.localStorage;
  private saveSubscriber: Subscription;
  private reloadSubscriber: Subscription;
  private updating = false;

  columnFilters: CdTableColumnFilter[] = [];
  selectedFilter: CdTableColumnFilter;
  get columnFiltered(): boolean {
    return _.some(this.columnFilters, (filter: any) => {
      return filter.value !== undefined;
    });
  }

  constructor(
    // private ngZone: NgZone,
    private cdRef: ChangeDetectorRef,
    private timerService: TimerService
  ) {}

  static prepareSearch(search: string) {
    search = search.toLowerCase().replace(/,/g, '');
    if (search.match(/['"][^'"]+['"]/)) {
      search = search.replace(/['"][^'"]+['"]/g, (match: string) => {
        return match.replace(/(['"])([^'"]+)(['"])/g, '$2').replace(/ /g, '+');
      });
    }
    return search.split(' ').filter((word) => word);
  }

  ngAfterViewInit(): void {
    if (this.showInlineActions && this.tableActions?.dropDownActions?.length) {
      this.tableColumns = [
        ...this.tableColumns,
        {
          name: '',
          prop: '',
          className: 'w25',
          sortable: false,
          cellTemplate: this.tableActionTpl
        }
      ];
    }

    const tableHeadersSubscription = this._tableHeaders
      .pipe(
        map((values: CdTableColumn[]) =>
          values.map(
            (col: CdTableColumn) =>
              new TableHeaderItem({
                data: col?.headerTemplate ? { ...col } : col.name,
                title: col.name,
                template: col?.headerTemplate,
                // if cellClass is a function it cannot be called here as it requires table data to execute
                // instead if cellClass is a function it will be called and applied while parsing the data
                className: _.isString(col?.cellClass) ? `${col?.cellClass}` : `${col?.className}`,
                visible: !col.isHidden,
                sortable: _.isNil(col.sortable) ? true : col.sortable
              })
          )
        )
      )
      .subscribe({
        next: (values: TableHeaderItem[]) => (this.model.header = values)
      });

    const datasetSubscription = this._dataset
      .pipe(
        filter((values: any[]) => {
          if (!values?.length) {
            this.model.data = [];
            return false;
          }
          return true;
        })
      )
      .subscribe({
        next: (values) => {
          const datasets: TableItem[][] = values.map((val) => {
            return this.tableColumns.map((column: CdTableColumn, colIndex: number) => {
              const rowValue = _.get(val, column?.prop);

              const pipeTransform = () =>
                column?.prop ? column.pipe.transform(rowValue) : column.pipe.transform(val);

              let tableItem = new TableItem({
                selected: val,
                data: {
                  value: column.pipe ? pipeTransform() : rowValue,
                  row: val,
                  column: { ...column, ...val }
                }
              });

              if (colIndex === 0) {
                tableItem.data = { ...tableItem.data, row: val };

                if (this.hasDetails) {
                  tableItem.expandedData = val;
                  tableItem.expandedTemplate = this.rowDetailTpl;
                }
              }

              if (column.cellClass && _.isFunction(column.cellClass)) {
                this.model.header[colIndex].className = column.cellClass({
                  row: val,
                  column,
                  value: rowValue
                });
              }

              tableItem.template = column.cellTemplate || this.defaultValueTpl;
              return tableItem;
            });
          });
          if (!_.isEqual(this.model.data, datasets)) {
            this.model.data = datasets;
          }
        }
      });

    const rowsExpandedSubscription = this.model.rowsExpandedChange.subscribe({
      next: (index: number) => {
        if (this.model.rowsExpanded.every((x) => !x)) {
          this.expanded = undefined;
        } else {
          this.expanded = _.get(this.model.data?.[index], [0, 'selected']);
          this.model.rowsExpanded = this.model.rowsExpanded.map(
            (_, rowIndex: number) => rowIndex === index
          );
        }
      }
    });

    const rowsChangeSubscription = this.model.rowsSelectedChange.subscribe(() =>
      this.updateSelectAllCheckbox()
    );
    const dataChangeSubscription = this.model.dataChange.subscribe(() => {
      this.updateSelectAllCheckbox();
    });

    this._subscriptions.add(tableHeadersSubscription);
    this._subscriptions.add(datasetSubscription);
    this._subscriptions.add(rowsExpandedSubscription);
    this._subscriptions.add(rowsChangeSubscription);
    this._subscriptions.add(dataChangeSubscription);
  }

  ngOnInit() {
    this.localColumns = _.clone(this.columns);
    // debounce reloadData method so that search doesn't run api requests
    // for every keystroke
    if (this.serverSide) {
      this.reloadData = _.debounce(this.reloadData, 1000);
    }

    // ngx-datatable triggers calculations each time mouse enters a row,
    // this will prevent that.
    // this.table.element.addEventListener('mouseenter', (e) => e.stopPropagation());
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
      const loadingSubscription = this.fetchData.subscribe(() => {
        this.loadingIndicator = false;
        this.cdRef.detectChanges();
      });
      this._subscriptions.add(loadingSubscription);
    }

    if (_.isInteger(this.autoReload) && this.autoReload > 0) {
      this.reloadSubscriber = this.timerService
        .get(() => of(0), this.autoReload)
        .subscribe(() => {
          this.reloadData();
        });
    } else if (!this.autoReload) {
      this.reloadData();
    } else {
      this.useData();
    }
  }
  onRowDetailHover(event: any) {
    event.target
      .closest('tr')
      .previousElementSibling.classList.remove('cds--expandable-row--hover');
    event.target.closest('tr').previousElementSibling.classList.remove('cds--data-table--selected');
  }
  initUserConfig() {
    if (this.autoSave) {
      this.tableName = this._calculateUniqueTableName(this.localColumns);
      this._loadUserConfig();
      this._initUserConfigAutoSave();
    }
    if (this.limit !== TABLE_LIST_LIMIT || !this.userConfig.limit) {
      this.userConfig.limit = this.limit;
    }
    if (!(this.userConfig.offset >= 0)) {
      this.userConfig.offset = this.model.currentPage - 1;
    }
    if (!this.userConfig.search) {
      this.userConfig.search = this.search;
    }
    if (!this.userConfig.sorts) {
      this.userConfig.sorts = this.sorts;
    }
    if (!this.userConfig.columns) {
      this.updateUserColumns();
    } else {
      this.userConfig.columns.forEach((col) => {
        for (let i = 0; i < this.localColumns.length; i++) {
          if (this.localColumns[i].prop === col.prop) {
            this.localColumns[i].isHidden = col.isHidden;
          }
        }
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

  filterHiddenColumns() {
    this.tableColumns = this.localColumns;
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

  onSelectFilter(filter: string) {
    const value = this.columnFilters.find((x) => x.column.name === filter);
    this.selectedFilter = value;
  }

  onChangeFilter(filter: string) {
    const option = this.selectedFilter.options.find((x) => x.raw === filter);
    this.selectedFilter.value = _.isEqual(this.selectedFilter.value, option) ? undefined : option;
    this.updateFilter();
  }

  doColumnFiltering() {
    const appliedFilters: CdTableColumnFiltersChange['filters'] = [];
    let data = _.isArray(this.data) ? [...this.data] : [];
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
        const value = _.get(row, filter.column.prop);
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
        this.updateSelection.emit(_.clone(this.selection));
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
    this._subscriptions.unsubscribe();
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
    this.cellTemplates.timeAgo = this.timeAgoTpl;
    this.cellTemplates.path = this.pathTpl;
    this.cellTemplates.tooltip = this.tooltipTpl;
    this.cellTemplates.copy = this.copyTpl;
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

  ngOnChanges(changes: SimpleChanges) {
    if (changes?.data?.currentValue) {
      this.useData();
    }
  }

  setLimit(e: any) {
    const value = Number(e.target.value);
    if (value > 0) {
      if (this.maxLimit && value > this.maxLimit) {
        this.userConfig.limit = this.maxLimit;
        // change input field to maxLimit
        e.srcElement.value = this.maxLimit;
      } else {
        this.userConfig.limit = value;
      }
    }
    if (this.serverSide) {
      this.reloadData();
    }
  }

  reloadData() {
    if (!this.updating) {
      this.status = new TableStatus();
      const context = new CdTableFetchDataContext(() => {
        // Do we have to display the error panel?
        if (!!context.errorConfig.displayError) {
          this.status = new TableStatus('danger', $localize`Failed to load data.`);
        }
        // Force data table to show no data?
        if (context.errorConfig.resetData) {
          this.data = [];
        }
        // Stop the loading indicator and reset the data table
        // to the correct state.
        this.useData();
      });
      context.pageInfo.offset = this.userConfig.offset;
      context.pageInfo.limit = this.userConfig.limit;
      context.search = this.userConfig.search;
      if (this.userConfig.sorts?.length) {
        const sort = this.userConfig.sorts[0];
        context.sort = `${sort.dir === 'desc' ? '-' : '+'}${sort.prop}`;
      }
      this.fetchData.emit(context);
      this.updating = true;
    }
  }

  refreshBtn() {
    this.loadingIndicator = true;
    this.reloadData();
  }

  onPageChange(page: number) {
    this.model.currentPage = page;

    this.userConfig.offset = this.model.currentPage - 1;
    this.userConfig.limit = this.model.pageLength;

    if (this.serverSide) {
      this.reloadData();
      return;
    }

    this.doPagination({});
  }

  doPagination({
    page = this.model.currentPage,
    size = this.model.pageLength,
    filteredData = this.rows
  }): void {
    if (this.serverSide) {
      this._dataset.next(filteredData);
      return;
    }

    if (this.limit === 0) {
      this.model.currentPage = 1;
      this.model.pageLength = filteredData.length || 1;
      this._dataset.next(filteredData);
      return;
    }
    const { start, end } = this.paginate({ page, size, filteredData });

    const paginated = filteredData?.slice?.(start, end);

    this._dataset.next(paginated);
  }

  /**
   * Pagination function
   */
  paginate = _.cond<TPaginationInput, TPaginationOutput>([
    [(x) => x.page <= 1, (x) => ({ start: 0, end: x.size })],
    [(x) => x.page >= x.filteredData.length, (x) => ({ start: 0, end: x.filteredData.length })],
    [
      (x) => x.page >= x.filteredData.length && x.page * x.size > x.filteredData.length,
      (x) => ({ start: 0, end: x.filteredData.length })
    ],
    [
      (x) => x.page * x.size > x.filteredData.length,
      (x) => ({ start: (x.page - 1) * x.size, end: x.filteredData.length })
    ],
    [_.stubTrue, (x) => ({ start: (x.page - 1) * x.size, end: x.page * x.size })]
  ]);

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
    this.doSorting();
    this.updateSelected();
    this.updateExpanded();
    this.toggleExpandRow();
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
    if (!this.selection?.selected?.length) return;

    const newSelected = new Set();
    this.selection.selected.forEach((selectedItem) => {
      for (const row of this.data) {
        if (selectedItem[this.identifier] === row[this.identifier]) {
          newSelected.add(row);
        }
      }
    });
    if (newSelected.size === 0) return;
    const newSelectedArray = Array.from(newSelected.values());

    newSelectedArray?.forEach?.((selection: any) => {
      const rowIndex = this.model.data.findIndex(
        (row: TableItem[]) =>
          _.get(row, [0, 'selected', this.identifier]) === selection[this.identifier]
      );
      rowIndex > -1 && this.model.selectRow(rowIndex, true);
    });

    if (
      this.updateSelectionOnRefresh === 'onChange' &&
      _.isEqual(this.selection.selected, newSelectedArray)
    ) {
      return;
    }

    this.selection.selected = newSelectedArray;

    if (this.updateSelectionOnRefresh === 'never') {
      return;
    }

    this.updateSelection.emit(_.clone(this.selection));
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
  }

  _toggleSelection(rowIndex: number, isSelected: boolean) {
    const selectedData = _.get(this.model.data?.[rowIndex], [0, 'selected']);
    if (isSelected) {
      this.selection.selected = [...this.selection.selected, selectedData];
    } else {
      this.selection.selected = this.selection.selected.filter(
        (s) => s[this.identifier] !== selectedData[this.identifier]
      );
    }
  }

  onSelect(selectedRowIndex: number) {
    const selectedData = _.get(this.model.data?.[selectedRowIndex], [0, 'selected']);
    if (this.selectionType === 'single') {
      this.model.selectAll(false);
      this.selection.selected = [selectedData];
    } else {
      this.selection.selected = [...this.selection.selected, selectedData];
    }
    this.model.selectRow(selectedRowIndex, true);
    this.updateSelection.emit(this.selection);
  }

  onSelectAll() {
    this.model.selectAll(!this.selectAllCheckbox && !this.selectAllCheckboxSomeSelected);
    this.model.rowsSelected.forEach((isSelected: boolean, rowIndex: number) =>
      this._toggleSelection(rowIndex, isSelected)
    );
    this.updateSelection.emit(this.selection);
    this.cdRef.detectChanges();
  }

  onDeselect(deselectedRowIndex: number) {
    this.model.selectRow(deselectedRowIndex, false);
    if (this.selectionType === 'single') {
      return;
    }
    this._toggleSelection(deselectedRowIndex, false);
    this.updateSelection.emit(this.selection);
  }

  onDeselectAll() {
    this.model.selectAll(false);
    this.model.rowsSelected.forEach((isSelected: boolean, rowIndex: number) =>
      this._toggleSelection(rowIndex, isSelected)
    );
    this.updateSelection.emit(this.selection);
  }

  onBatchActionsCancel() {
    this.model.selectAll(false);
    this.model.rowsSelected.forEach((_isSelected: boolean, rowIndex: number) =>
      this._toggleSelection(rowIndex, false)
    );
  }

  toggleColumn(column: CdTableColumn) {
    const prop: string | number = column.prop;
    const hide = !column.isHidden;
    if (hide && this.visibleColumns.length === 1) {
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
    if (!_.find(this.visibleColumns, (c: CdTableColumn) => c.prop === sortProp)) {
      this.userConfig.sorts = this.createSortingDefinition(this.visibleColumns[0].prop);
    }
    if (this.showInlineActions && this.tableActions?.dropDownActions?.length) {
      this.tableColumns = [
        ...this.tableColumns,
        {
          name: '',
          prop: '',
          className: 'w25',
          sortable: false,
          cellTemplate: this.tableActionTpl
        }
      ];
    }
    this.cdRef.detectChanges();
  }

  createSortingDefinition(prop: string | number): CdSortPropDir[] {
    return [
      {
        prop: prop,
        dir: CdSortDirection.asc
      }
    ];
  }

  changeSorting(columnIndex: number) {
    if (!this.model?.header?.[columnIndex]) {
      return;
    }

    const prop = this.tableColumns?.[columnIndex]?.prop;

    if (this.model.header[columnIndex].sorted) {
      this.model.header[columnIndex].descending = this.model.header[columnIndex].ascending;
    } else {
      const configDir = this.userConfig?.sorts?.find?.((x) => x.prop === prop)?.dir;
      this.model.header[columnIndex].ascending = configDir === 'asc';
      this.model.header[columnIndex].descending = configDir === 'desc';
    }

    const dir = this.model.header[columnIndex].ascending
      ? CdSortDirection.asc
      : CdSortDirection.desc;
    const sorts = [{ dir, prop }];

    this.userConfig.sorts = sorts;
    if (this.serverSide) {
      this.userConfig.offset = 0;
      this.reloadData();
    }

    this.doSorting(columnIndex);
  }

  doSorting(columnIndex?: number) {
    const index =
      columnIndex ||
      this.visibleColumns?.findIndex?.((x) => x.prop === this.userConfig?.sorts?.[0]?.prop);

    if (_.isNil(index) || index < 0 || !this.model?.header?.[index]) {
      return;
    }

    const prop = this.tableColumns?.[index]?.prop;

    const configDir = this.userConfig?.sorts?.find?.((x) => x.prop === prop)?.dir;
    this.model.header[index].ascending = configDir === 'asc';
    this.model.header[index].descending = configDir === 'desc';

    const tmp = this.rows.slice();

    tmp.sort((a, b) => {
      const rowA = _.get(a, prop);
      const rowB = _.get(b, prop);
      if (rowA > rowB) {
        return this.model.header[index].descending ? -1 : 1;
      }
      if (rowB > rowA) {
        return this.model.header[index].descending ? 1 : -1;
      }
      return 0;
    });

    this.model.header[index].sorted = true;
    this.rows = tmp.slice();
  }

  onClearSearch() {
    this.search = '';
    this.expanded = undefined;
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
    if (this.serverSide) {
      if (this.userConfig.search !== this.search) {
        // if we don't go back to the first page it will try load
        // a page which could not exists with an especific search
        this.userConfig.offset = 0;
        this.userConfig.limit = this.limit;
        this.userConfig.search = this.search;
        this.updating = false;
        this.reloadData();
      }
      this.rows = this.data;
    } else {
      let rows = this.columnFilters.length !== 0 ? this.doColumnFiltering() : this.data;

      if (this.search.length > 0 && rows?.length) {
        const columns = this.localColumns.filter(
          (c) => c.cellTransformation !== CellTemplate.sparkline
        );
        // update the rows
        rows = this.subSearch(rows, TableComponent.prepareSearch(this.search), columns);
      }

      this.rows = rows;
    }
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

  toggleExpandRow() {
    if (_.isNil(this.expanded)) {
      return;
    }

    const expandedRowIndex = this.model.data.findIndex((row: TableItem[]) => {
      const rowSelectedId = _.get(row, [0, 'selected', this.identifier]);
      const expandedId = this.expanded?.[this.identifier];
      return _.isEqual(rowSelectedId, expandedId);
    });

    if (expandedRowIndex < 0) {
      return;
    }

    this.model.rowsExpanded = this.model.rowsExpanded.map(
      (_, rowIndex: number) => rowIndex === expandedRowIndex
    );
  }

  firstExpandedDataInRow(row: TableItem[]) {
    const found = row.find((d) => d.expandedData);
    if (found) {
      return found.expandedData;
    }
    return found;
  }

  shouldExpandAsTable(row: TableItem[]) {
    return row.some((d) => d.expandAsTable);
  }

  isRowExpandable(index: number) {
    return this.model.data[index].some((d) => d && d.expandedData);
  }

  trackByFn(id: string, _index: number, row: TableItem[]) {
    const uniqueIdentifier = _.get(row, [0, 'data', 'row', id])?.toString?.();
    return uniqueIdentifier || row;
  }

  updateSelectAllCheckbox() {
    const selectedRowsCount = this.model.selectedRowsCount();

    if (selectedRowsCount <= 0) {
      // reset select all checkbox if nothing selected
      this.selectAllCheckbox = false;
      this.selectAllCheckboxSomeSelected = false;
    } else if (selectedRowsCount < this.model.data.length) {
      this.selectAllCheckbox = true;
      this.selectAllCheckboxSomeSelected = true;
    } else {
      this.selectAllCheckbox = true;
      this.selectAllCheckboxSomeSelected = false;
    }
  }
}

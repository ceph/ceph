import {
  AfterContentChecked,
  Component,
  ComponentFactoryResolver,
  EventEmitter,
  Input,
  OnChanges,
  OnInit,
  Output,
  TemplateRef,
  Type,
  ViewChild
} from '@angular/core';

import { DatatableComponent, SortDirection, SortPropDir } from '@swimlane/ngx-datatable';
import * as _ from 'lodash';

import { CdTableColumn } from '../../models/cd-table-column';
import { TableDetailsDirective } from '../table-details.directive';

@Component({
  selector: 'cd-table',
  templateUrl: './table.component.html',
  styleUrls: ['./table.component.scss']
})
export class TableComponent implements AfterContentChecked, OnInit, OnChanges {
  @ViewChild(DatatableComponent) table: DatatableComponent;
  @ViewChild(TableDetailsDirective) detailTemplate: TableDetailsDirective;
  @ViewChild('tableCellBoldTpl') tableCellBoldTpl: TemplateRef<any>;
  @ViewChild('sparklineTpl') sparklineTpl: TemplateRef<any>;
  @ViewChild('routerLinkTpl') routerLinkTpl: TemplateRef<any>;
  @ViewChild('perSecondTpl') perSecondTpl: TemplateRef<any>;

  // This is the array with the items to be shown.
  @Input() data: any[] = [];
  // Each item -> { prop: 'attribute name', name: 'display name' }
  @Input() columns: CdTableColumn[];
  // Each item -> { prop: 'attribute name', dir: 'asc'||'desc'}
  @Input() sorts?: SortPropDir[];
  // Method used for setting column widths.
  @Input() columnMode ?= 'force';
  // Name of the component e.g. 'TableDetailsComponent'
  @Input() detailsComponent?: string;
  // Display the tool header, including reload button, pagination and search fields?
  @Input() toolHeader ?= true;
  // Display the table header?
  @Input() header ?= true;
  // Display the table footer?
  @Input() footer ?= true;
  // Page size to show. Set to 0 to show unlimited number of rows.
  @Input() limit ?= 10;
  // An optional function that is called before the details page is show.
  // The current selection is passed as function argument. To do not display
  // the details page, return false.
  @Input() beforeShowDetails: Function;

  // Should be the function that will update the input data.
  @Output() fetchData = new EventEmitter();

  cellTemplates: {
    [key: string]: TemplateRef<any>
  } = {};
  selectionType: string = undefined;
  search = '';
  rows = [];
  selected = [];
  loadingIndicator = false;
  paginationClasses = {
    pagerLeftArrow: 'i fa fa-angle-double-left',
    pagerRightArrow: 'i fa fa-angle-double-right',
    pagerPrevious: 'i fa fa-angle-left',
    pagerNext: 'i fa fa-angle-right'
  };

  // Internal variable to check if it is necessary to recalculate the
  // table columns after the browser window has been resized.
  private currentWidth: number;

  constructor(private componentFactoryResolver: ComponentFactoryResolver) { }

  ngOnInit() {
    this._addTemplates();
    this.columns.map((column) => {
      if (column.cellTransformation) {
        column.cellTemplate = this.cellTemplates[column.cellTransformation];
      }
      return column;
    });
    this.reloadData();
    if (this.detailsComponent) {
      this.selectionType = 'multi';
    }
    if (!this.sorts) {
      this.sorts = [
        {
          prop: this.columns[0].prop,
          dir: SortDirection.asc
        }
      ];
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
      this.table.recalculate();
    }
  }

  _addTemplates () {
    this.cellTemplates.bold = this.tableCellBoldTpl;
    this.cellTemplates.sparkline = this.sparklineTpl;
    this.cellTemplates.routerLink = this.routerLinkTpl;
    this.cellTemplates.perSecond = this.perSecondTpl;
  }

  ngOnChanges(changes) {
    this.useData();
  }

  setLimit(e) {
    const value = parseInt(e.target.value, 10);
    if (value > 0) {
      this.limit = value;
    }
  }

  reloadData() {
    if (this.loadingIndicator) {
      return;
    }
    this.loadingIndicator = true;
    this.fetchData.emit();
  }

  useData() {
    if (!this.data) {
      return; // Wait for data
    }
    this.rows = [...this.data];
    this.loadingIndicator = false;
  }

  toggleExpandRow() {
    if (this.selected.length > 0) {
      this.table.rowDetail.toggleExpandRow(this.selected[0]);
    } else {
      this.detailTemplate.viewContainerRef.clear();
    }
  }

  updateDetailView() {
    if (!this.detailsComponent) {
      return;
    }
    if (_.isFunction(this.beforeShowDetails)) {
      if (!this.beforeShowDetails(this.selected)) {
        return;
      }
    }
    const factories = Array.from(this.componentFactoryResolver['_factories'].keys());
    const factoryClass = <Type<any>>factories.find((x: any) => x.name === this.detailsComponent);
    this.detailTemplate.viewContainerRef.clear();
    const cmpRef = this.detailTemplate.viewContainerRef.createComponent(
      this.componentFactoryResolver.resolveComponentFactory(factoryClass)
    );
    cmpRef.instance.selected = this.selected;
  }

  updateFilter(event?) {
    if (!event) {
      this.search = '';
    }
    const val = this.search.toLowerCase();
    const columns = this.columns;
    // update the rows
    this.rows = this.data.filter(function (d) {
      return columns.filter((c) => {
        return (typeof d[c.prop] === 'string' || typeof d[c.prop] === 'number')
          && (d[c.prop] + '').toLowerCase().indexOf(val) !== -1;
      }).length > 0;
    });
    // Whenever the filter changes, always go back to the first page
    this.table.offset = 0;
  }

  getRowClass() {
    // Return the function used to populate a row's CSS classes.
    return () => {
      return {
        'clickable': !_.isUndefined(this.detailsComponent)
      };
    };
  }
}

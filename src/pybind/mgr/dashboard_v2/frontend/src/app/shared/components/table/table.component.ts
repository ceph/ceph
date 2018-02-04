import {
  Component, ComponentFactoryResolver, EventEmitter, Input, OnChanges, OnInit, Output, Type, ViewChild
} from '@angular/core';

import {DatatableComponent, TableColumn} from '@swimlane/ngx-datatable';

import {TableDetailsDirective} from './table-details.directive';

@Component({
  selector: 'cd-table',
  templateUrl: './table.component.html',
  styleUrls: ['./table.component.scss']
})
export class TableComponent implements OnInit, OnChanges {
  @ViewChild(DatatableComponent) table: DatatableComponent;
  @ViewChild(TableDetailsDirective) detailTemplate: TableDetailsDirective;

  @Input() data: any[]; // This is the array with the items to be shown
  @Input() columns: TableColumn[]; // each item -> { prop: 'attribute name', name: 'display name' }
  @Input() detailsComponent?: string; // name of the component fe 'TableDetailsComponent'
  @Input() header ? = true;

  @Output() fetchData = new EventEmitter(); // Should be the function that will update the input data

  selectable: String = undefined;
  search = '';
  rows = [];
  selected = [];
  paginationClasses = {
    pagerLeftArrow: 'i fa fa-angle-double-left',
    pagerRightArrow: 'i fa fa-angle-double-right',
    pagerPrevious: 'i fa fa-angle-left',
    pagerNext: 'i fa fa-angle-right'
  };
  limit = 10;

  constructor(private componentFactoryResolver: ComponentFactoryResolver) {}

  ngOnInit() {
    this.reloadData();
    if (this.detailsComponent) {
      this.selectable = 'multi';
    }
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
    this.fetchData.emit();
  }

  useData() {
    this.rows = [...this.data];
  }

  toggleExpandRow() {
    if (this.selected.length > 0) {
      this.table.rowDetail.toggleExpandRow(this.selected[0]);
    }
  }

  updateDetailView() {
    if (!this.detailsComponent) {
      return;
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
}

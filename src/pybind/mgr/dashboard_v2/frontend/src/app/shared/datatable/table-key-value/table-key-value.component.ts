import { Component, EventEmitter, Input, OnChanges, OnInit, Output } from '@angular/core';

import * as _ from 'lodash';

import { CellTemplate } from '../../enum/cell-template.enum';
import { CdTableColumn } from '../../models/cd-table-column';

/**
 * Display the given data in a 2 column data table. The left column
 * shows the 'key' attribute, the right column the 'value' attribute.
 * The data table has the following characteristics:
 * - No header and footer is displayed
 * - The relation of the width for the columns 'key' and 'value' is 1:3
 * - The 'key' column is displayed in bold text
 */
@Component({
  selector: 'cd-table-key-value',
  templateUrl: './table-key-value.component.html',
  styleUrls: ['./table-key-value.component.scss']
})
export class TableKeyValueComponent implements OnInit, OnChanges {

  columns: Array<CdTableColumn> = [];

  @Input() data: any;

  tableData: {
    key: string,
    value: any
  }[];

  /**
   * The function that will be called to update the input data.
   */
  @Output() fetchData = new EventEmitter();

  constructor() { }

  ngOnInit() {
    this.columns = [
      {
        prop: 'key',
        flexGrow: 1,
        cellTransformation: CellTemplate.bold
      },
      {
        prop: 'value',
        flexGrow: 3
      }
    ];
    this.useData();
  }

  ngOnChanges(changes) {
    this.useData();
  }

  useData() {
    let temp = [];
    if (!this.data) {
      return; // Wait for data
    } else if (_.isArray(this.data)) {
      const first = this.data[0];
      if (_.isPlainObject(first) && _.has(first, 'key') && _.has(first, 'value')) {
        temp = [...this.data];
      } else {
        if (_.isArray(first)) {
          if (first.length === 2) {
            temp = this.data.map(a => ({
              key: a[0],
              value: a[1]
            }));
          } else {
            throw new Error('Wrong array format');
          }
        }
      }
    } else if (_.isPlainObject(this.data)) {
      temp = Object.keys(this.data).map(k => ({
        key: k,
        value: this.data[k]
      }));
    } else {
      throw new Error('Wrong data format');
    }
    this.tableData = temp.map(o => {
      if (_.isArray(o.value)) {
        o.value = o.value.join(', ');
      } else if (_.isObject(o.value)) {
        return;
      }
      return o;
    }).filter(o => o); // Filters out undefined
  }

  reloadData() {
    // Forward event triggered by the 'cd-table' datatable.
    this.fetchData.emit();
  }
}

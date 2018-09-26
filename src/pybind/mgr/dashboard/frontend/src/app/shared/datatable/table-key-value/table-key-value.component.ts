import {
  Component,
  EventEmitter,
  Input,
  OnChanges,
  OnInit,
  Output,
  ViewChild
} from '@angular/core';

import * as _ from 'lodash';

import { CellTemplate } from '../../enum/cell-template.enum';
import { CdTableColumn } from '../../models/cd-table-column';
import { TableComponent } from '../table/table.component';

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

  @ViewChild(TableComponent)
  table: TableComponent;

  @Input() data: any;
  @Input() autoReload: any = 5000;
  @Input() renderObjects = false;
  // Only used if objects are rendered
  @Input() appendParentKey = true;

  columns: Array<CdTableColumn> = [];
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
    // We need to subscribe the 'fetchData' event here and not in the
    // HTML template, otherwise the data table will display the loading
    // indicator infinitely if data is only bound via '[data]="xyz"'.
    // See for 'loadingIndicator' in 'TableComponent::ngOnInit()'.
    if (this.fetchData.observers.length > 0) {
      this.table.fetchData.subscribe(() => {
        // Forward event triggered by the 'cd-table' data table.
        this.fetchData.emit();
      });
    }
    this.useData();
  }

  ngOnChanges(changes) {
    this.useData();
  }

  useData() {
    if (!this.data) {
      return; // Wait for data
    }
    this.tableData = this._makePairs(this.data);
  }

  _makePairs(data: any) {
    let temp = [];
    if (!data) {
      return; // Wait for data
    } else if (_.isArray(data)) {
      temp = this._makePairsFromArray(data);
    } else if (_.isPlainObject(data)) {
      temp = this._makePairsFromObject(data);
    } else {
      throw new Error('Wrong data format');
    }
    temp = temp.map((v) => this._convertValue(v)).filter(o => o); // Filters out undefined
    return this.renderObjects ? this._insertFlattenObjects(temp) : temp;
  }

  _makePairsFromArray(data: any[]) {
    let temp = [];
    const first = data[0];
    if (_.isPlainObject(first)) {
      if (_.has(first, 'key') && _.has(first, 'value')) {
        temp = [...data];
      } else {
        throw new Error('Wrong object array format: {key: string, value: any}[]');
      }
    } else {
      if (_.isArray(first)) {
        if (first.length === 2) {
          temp = data.map(a => ({
            key: a[0],
            value: a[1]
          }));
        } else {
          throw new Error('Wrong array format: [string, any][]');
        }
      }
    }
    return temp;
  }

  _makePairsFromObject(data: object) {
    return Object.keys(data).map(k => ({
      key: k,
      value: data[k]
    }));
  }

  _insertFlattenObjects(temp: any[]) {
    temp.forEach((v, i) => {
      if (_.isPlainObject(v.value)) {
        temp.splice(i, 1);
        this._makePairs(v.value).forEach(item => {
          if (this.appendParentKey) {
            item.key = v.key + ' ' + item.key;
          }
          temp.splice(i, 0, item);
          i++;
        });
      }
    });
    return temp;
  }

  _convertValue(v: any) {
    if (_.isArray(v.value)) {
      v.value = v.value.join(', ');
    } else if (_.isPlainObject(v.value) && !this.renderObjects) {
      return;
    }
    return v;
  }
}

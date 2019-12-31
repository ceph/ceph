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
import { CdDatePipe } from '../../pipes/cd-date.pipe';
import { TableComponent } from '../table/table.component';

interface KeyValueItem {
  key: string;
  value: any;
}

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
  @ViewChild(TableComponent, { static: true })
  table: TableComponent;

  @Input()
  data: any;
  @Input()
  autoReload: any = 5000;
  @Input()
  renderObjects = false;
  // Only used if objects are rendered
  @Input()
  appendParentKey = true;
  @Input()
  hideEmpty = false;
  @Input()
  hideKeys = []; // Keys of pairs not to be displayed

  // If set, the classAddingTpl is used to enable different css for different values
  @Input()
  customCss?: { [css: string]: number | string | ((any) => boolean) };

  columns: Array<CdTableColumn> = [];
  tableData: KeyValueItem[];

  /**
   * The function that will be called to update the input data.
   */
  @Output()
  fetchData = new EventEmitter();

  constructor(private datePipe: CdDatePipe) {}

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
    if (this.customCss) {
      this.columns[1].cellTransformation = CellTemplate.classAdding;
    }
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

  ngOnChanges() {
    this.useData();
  }

  useData() {
    if (!this.data) {
      return; // Wait for data
    }
    let pairs = this.makePairs(this.data);
    if (this.hideKeys) {
      pairs = pairs.filter((pair) => !this.hideKeys.includes(pair.key));
    }
    this.tableData = pairs;
  }

  private makePairs(data: any): KeyValueItem[] {
    let result: KeyValueItem[] = [];
    if (!data) {
      return; // Wait for data
    } else if (_.isArray(data)) {
      result = this.makePairsFromArray(data);
    } else if (_.isObject(data)) {
      result = this.makePairsFromObject(data);
    } else {
      throw new Error('Wrong data format');
    }
    result = result
      .map((item) => {
        item.value = this.convertValue(item.value);
        return item;
      })
      .filter((i) => i.value !== null);
    return _.sortBy(this.renderObjects ? this.insertFlattenObjects(result) : result, 'key');
  }

  private makePairsFromArray(data: any[]): KeyValueItem[] {
    let temp = [];
    const first = data[0];
    if (_.isArray(first)) {
      if (first.length === 2) {
        temp = data.map((a) => ({
          key: a[0],
          value: a[1]
        }));
      } else {
        throw new Error(
          `Array contains too many elements (${first.length}). ` +
            `Needs to be of type [string, any][]`
        );
      }
    } else if (_.isObject(first)) {
      if (_.has(first, 'key') && _.has(first, 'value')) {
        temp = [...data];
      } else {
        temp = data.reduce(
          (previous: any[], item) => previous.concat(this.makePairsFromObject(item)),
          temp
        );
      }
    }
    return temp;
  }

  private makePairsFromObject(data: object): KeyValueItem[] {
    return Object.keys(data).map((k) => ({
      key: k,
      value: data[k]
    }));
  }

  private insertFlattenObjects(data: KeyValueItem[]): any[] {
    return _.flattenDeep(
      data.map((item) => {
        const value = item.value;
        const isObject = _.isObject(value);
        if (!isObject || _.isEmpty(value)) {
          if (isObject) {
            item.value = '';
          }
          return item;
        }
        return this.splitItemIntoItems(item);
      })
    );
  }

  /**
   * Split item into items will call _makePairs inside _makePairs (recursion), in oder to split
   * the object item up into items as planned.
   */
  private splitItemIntoItems(data: { key: string; value: object }): KeyValueItem[] {
    return this.makePairs(data.value).map((item) => {
      if (this.appendParentKey) {
        item.key = data.key + ' ' + item.key;
      }
      return item;
    });
  }

  private convertValue(value: any): KeyValueItem {
    if (_.isArray(value)) {
      if (_.isEmpty(value) && this.hideEmpty) {
        return null;
      }
      value = value.map((item) => (_.isObject(item) ? JSON.stringify(item) : item)).join(', ');
    } else if (_.isObject(value)) {
      if ((this.hideEmpty && _.isEmpty(value)) || !this.renderObjects) {
        return null;
      }
    } else if (_.isString(value)) {
      if (value === '' && this.hideEmpty) {
        return null;
      }
      if (this.isDate(value)) {
        value = this.datePipe.transform(value) || value;
      }
    }

    return value;
  }

  private isDate(s) {
    const sep = '[ -:.TZ]';
    const n = '\\d{2}' + sep;
    //                            year     -    m - d - h : m : s . someRest  Z (if UTC)
    return s.match(new RegExp('^\\d{4}' + sep + n + n + n + n + n + '\\d*' + 'Z?$'));
  }
}

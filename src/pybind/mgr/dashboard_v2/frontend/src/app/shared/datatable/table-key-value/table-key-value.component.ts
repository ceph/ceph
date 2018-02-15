import { Component, EventEmitter, Input, OnInit, Output } from '@angular/core';

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
export class TableKeyValueComponent implements OnInit {

  columns: Array<CdTableColumn> = [];

  /**
   * An array of objects to be displayed in the data table.
   */
  @Input() data: Array<object> = [];

  /**
   * The name of the attribute to be displayed as key.
   * Defaults to 'key'.
   * @type {string}
   */
  @Input() key = 'key';

  /**
   * The name of the attribute to be displayed as value.
   * Defaults to 'value'.
   * @type {string}
   */
  @Input() value = 'value';

  /**
   * The function that will be called to update the input data.
   */
  @Output() fetchData = new EventEmitter();

  constructor() { }

  ngOnInit() {
    this.columns = [
      {
        prop: this.key,
        flexGrow: 1,
        cellTransformation: CellTemplate.bold
      },
      {
        prop: this.value,
        flexGrow: 3
      }
    ];
  }

  reloadData() {
    // Forward event triggered by the 'cd-table' datatable.
    this.fetchData.emit();
  }
}

import { Component, ViewChild } from '@angular/core';

import { RgwBucketService } from '../../../shared/api/rgw-bucket.service';
import { TableComponent } from '../../../shared/datatable/table/table.component';
import { CdTableColumn } from '../../../shared/models/cd-table-column';
import { CdTableSelection } from '../../../shared/models/cd-table-selection';

@Component({
  selector: 'cd-rgw-bucket-list',
  templateUrl: './rgw-bucket-list.component.html',
  styleUrls: ['./rgw-bucket-list.component.scss']
})
export class RgwBucketListComponent {
  @ViewChild('table') table: TableComponent;

  columns: CdTableColumn[] = [];
  buckets: object[] = [];
  selection: CdTableSelection = new CdTableSelection();

  constructor(private rgwBucketService: RgwBucketService) {
    this.columns = [
      {
        name: 'Name',
        prop: 'bucket',
        flexGrow: 1
      },
      {
        name: 'Owner',
        prop: 'owner',
        flexGrow: 1
      }
    ];
  }

  getBucketList() {
    this.rgwBucketService.list()
      .subscribe((resp: object[]) => {
        this.buckets = resp;
      }, () => {
        // Force datatable to hide the loading indicator in
        // case of an error.
        this.buckets = [];
      });
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }
}

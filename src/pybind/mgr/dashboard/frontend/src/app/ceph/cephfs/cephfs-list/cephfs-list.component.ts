import { Component, OnInit } from '@angular/core';

import * as _ from 'lodash';

import { CephfsService } from '../../../shared/api/cephfs.service';
import { CdTableColumn } from '../../../shared/models/cd-table-column';
import { CdTableSelection } from '../../../shared/models/cd-table-selection';

@Component({
  selector: 'cd-cephfs-list',
  templateUrl: './cephfs-list.component.html',
  styleUrls: ['./cephfs-list.component.scss']
})
export class CephfsListComponent implements OnInit {
  columns: CdTableColumn[];
  filesystems: any = [];
  selection = new CdTableSelection();

  constructor(private cephfsService: CephfsService) {}

  ngOnInit() {
    this.columns = [
      {
        name: 'Name',
        prop: 'mdsmap.fs_name',
        flexGrow: 2
      },
      {
        name: 'Created',
        prop: 'mdsmap.created',
        flexGrow: 2
      },
      {
        name: 'Enabled',
        prop: 'mdsmap.enabled',
        flexGrow: 1
      }
    ];
  }

  loadFilesystems() {
    this.cephfsService.list().subscribe((resp: any[]) => {
      this.filesystems = resp;
    });
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }
}

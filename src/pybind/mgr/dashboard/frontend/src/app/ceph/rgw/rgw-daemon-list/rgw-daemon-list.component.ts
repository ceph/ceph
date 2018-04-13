import { Component } from '@angular/core';

import { RgwDaemonService } from '../../../shared/api/rgw-daemon.service';
import { CdTableColumn } from '../../../shared/models/cd-table-column';
import { CdTableSelection } from '../../../shared/models/cd-table-selection';
import { CephShortVersionPipe } from '../../../shared/pipes/ceph-short-version.pipe';

@Component({
  selector: 'cd-rgw-daemon-list',
  templateUrl: './rgw-daemon-list.component.html',
  styleUrls: ['./rgw-daemon-list.component.scss']
})
export class RgwDaemonListComponent {

  columns: CdTableColumn[] = [];
  daemons: object[] = [];
  selection: CdTableSelection = new CdTableSelection();

  constructor(private rgwDaemonService: RgwDaemonService,
              cephShortVersionPipe: CephShortVersionPipe) {
    this.columns = [
      {
        name: 'ID',
        prop: 'id',
        flexGrow: 2
      },
      {
        name: 'Hostname',
        prop: 'server_hostname',
        flexGrow: 2
      },
      {
        name: 'Version',
        prop: 'version',
        flexGrow: 1,
        pipe: cephShortVersionPipe
      }
    ];
  }

  getDaemonList() {
    this.rgwDaemonService.list()
      .subscribe((resp: object[]) => {
        this.daemons = resp;
      }, () => {
        // Force datatable to hide the loading indicator in
        // case of an error.
        this.daemons = [];
      });
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }
}

import { Component } from '@angular/core';

import { CdTableColumn } from '../../../shared/models/cd-table-column';
import { CdTableSelection } from '../../../shared/models/cd-table-selection';
import { CephShortVersionPipe } from '../../../shared/pipes/ceph-short-version.pipe';
import { RgwDaemonService } from '../services/rgw-daemon.service';

@Component({
  selector: 'cd-rgw-daemon-list',
  templateUrl: './rgw-daemon-list.component.html',
  styleUrls: ['./rgw-daemon-list.component.scss']
})
export class RgwDaemonListComponent {

  columns: Array<CdTableColumn> = [];
  daemons: Array<object> = [];
  selection = new CdTableSelection();

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
      .then((resp) => {
        this.daemons = resp;
      });
  }

  updateSelection(selection: CdTableSelection) {
    this.selection = selection;
  }
}

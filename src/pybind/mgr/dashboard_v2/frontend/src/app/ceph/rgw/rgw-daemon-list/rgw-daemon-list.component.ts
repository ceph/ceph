import { Component, OnInit } from '@angular/core';

import { CdTableColumn } from '../../../shared/models/cd-table-column';
import { CephShortVersionPipe } from '../../../shared/pipes/ceph-short-version.pipe';
import { RgwDaemonService } from '../services/rgw-daemon.service';

@Component({
  selector: 'cd-rgw-daemon-list',
  templateUrl: './rgw-daemon-list.component.html',
  styleUrls: ['./rgw-daemon-list.component.scss']
})
export class RgwDaemonListComponent implements OnInit {

  private columns: Array<CdTableColumn> = [];
  private daemons: Array<object> = [];

  detailsComponent = 'RgwDaemonDetailsComponent';

  constructor(private rgwDaemonService: RgwDaemonService) {
    this.columns = [
      {
        name: 'ID',
        prop: 'id',
        width: 100
      },
      {
        name: 'Hostname',
        prop: 'server_hostname',
        width: 100
      },
      {
        name: 'Version',
        prop: 'version',
        width: 50,
        pipe: new CephShortVersionPipe()
      }
    ];
  }

  ngOnInit() {
    this.getDaemonList();
  }

  getDaemonList() {
    this.rgwDaemonService.list()
      .then((resp) => {
        this.daemons = resp;
      });
  }

  beforeShowDetails(selected: Array<object>) {
    return selected.length === 1;
  }
}

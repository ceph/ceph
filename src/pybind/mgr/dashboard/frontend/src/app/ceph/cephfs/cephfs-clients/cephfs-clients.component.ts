import { Component, Input, OnInit } from '@angular/core';

import { I18n } from '@ngx-translate/i18n-polyfill';

import { CephfsService } from '../../../shared/api/cephfs.service';
import { ViewCacheStatus } from '../../../shared/enum/view-cache-status.enum';

@Component({
  selector: 'cd-cephfs-clients',
  templateUrl: './cephfs-clients.component.html',
  styleUrls: ['./cephfs-clients.component.scss']
})
export class CephfsClientsComponent implements OnInit {
  @Input()
  id: number;

  clients: any;
  viewCacheStatus: ViewCacheStatus;

  constructor(private cephfsService: CephfsService, private i18n: I18n) {}

  ngOnInit() {
    this.clients = {
      columns: [
        { prop: 'id', name: this.i18n('id') },
        { prop: 'type', name: this.i18n('type') },
        { prop: 'state', name: this.i18n('state') },
        { prop: 'version', name: this.i18n('version') },
        { prop: 'hostname', name: this.i18n('Host') },
        { prop: 'root', name: this.i18n('root') }
      ],
      data: []
    };

    this.clients.data = [];
    this.viewCacheStatus = ViewCacheStatus.ValueNone;
  }

  refresh() {
    this.cephfsService.getClients(this.id).subscribe((data: any) => {
      this.viewCacheStatus = data.status;
      this.clients.data = data.data;
    });
  }
}

import { Component, Input, OnInit } from '@angular/core';

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

  constructor(private cephfsService: CephfsService) {}

  ngOnInit() {
    this.clients = {
      columns: [
        { prop: 'id' },
        { prop: 'type' },
        { prop: 'state' },
        { prop: 'version' },
        { prop: 'hostname', name: 'Host' },
        { prop: 'root' }
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

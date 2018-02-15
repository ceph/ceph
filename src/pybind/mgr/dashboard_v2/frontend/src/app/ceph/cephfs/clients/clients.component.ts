import { Component, OnDestroy, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';

import { ViewCacheStatus } from '../../../shared/enum/view-cache-status.enum';
import { CephfsService } from '../cephfs.service';

@Component({
  selector: 'cd-clients',
  templateUrl: './clients.component.html',
  styleUrls: ['./clients.component.scss']
})
export class ClientsComponent implements OnInit, OnDestroy {
  routeParamsSubscribe: any;

  id: number;
  name: string;
  clients: any;
  viewCacheStatus: ViewCacheStatus;

  interval: any;

  constructor(private route: ActivatedRoute, private cephfsService: CephfsService) {}

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

    this.routeParamsSubscribe = this.route.params.subscribe((params: { id: number }) => {
      this.id = params.id;
      this.clients.data = [];
      this.viewCacheStatus = ViewCacheStatus.ValueNone;

      this.cephfsService.getCephfs(this.id).subscribe((data: any) => {
        this.name = data.cephfs.name;
      });

      this.refresh();
    });

    this.interval = setInterval(() => {
      this.refresh();
    }, 5000);
  }

  ngOnDestroy() {
    clearInterval(this.interval);
    this.routeParamsSubscribe.unsubscribe();
  }

  refresh() {
    this.cephfsService.getClients(this.id).subscribe((data: any) => {
      this.viewCacheStatus = data.status;
      this.clients.data = data.data;
    });
  }
}

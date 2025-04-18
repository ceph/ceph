import { Component, OnDestroy, OnInit, TemplateRef, ViewChild } from '@angular/core';
import { Router } from '@angular/router';
import { Subscription } from 'rxjs';
import { RbdMirroringService } from '~/app/shared/api/rbd-mirroring.service';
import { TableStatusViewCache } from '~/app/shared/classes/table-status-view-cache';
import { CephShortVersionPipe } from '~/app/shared/pipes/ceph-short-version.pipe';
@Component({
  selector: 'cd-mirroring-daemons',
  templateUrl: './daemon-list.component.html',
  styleUrls: ['./daemon-list.component.scss']
})
export class DaemonListComponent implements OnInit, OnDestroy {
  @ViewChild('healthTmpl', { static: true })
  healthTmpl: TemplateRef<any>;
  subs: Subscription;
  data: [];
  daemonData: any = [];
  columns: {};
  tableStatus = new TableStatusViewCache();

  constructor(
    private rbdMirroringService: RbdMirroringService,
    private cephShortVersionPipe: CephShortVersionPipe,
    private router: Router
  ) {}

  ngOnInit() {
    this.columns = [
      { prop: 'instance_id', name: $localize`Instance`, flexGrow: 2 },
      { prop: 'id', name: $localize`ID`, flexGrow: 2 },
      { prop: 'server_hostname', name: $localize`Hostname`, flexGrow: 2 },
      {
        prop: 'version',
        name: $localize`Version`,
        pipe: this.cephShortVersionPipe,
        flexGrow: 2
      },
      {
        prop: 'health',
        name: $localize`Health`,
        cellTemplate: this.healthTmpl,
        flexGrow: 1
      }
    ];

    this.subs = this.rbdMirroringService.subscribeSummary((data) => {
      this.data = data.content_data.daemons;
      this.daemonData = this.daemonCount(this.data);
      console.log(this.daemonData);
      this.tableStatus = new TableStatusViewCache(data.status);
    });
  }

  daemonCount(data: any) {
    const counts: any = {};
    const healthColorMap: any = {};
    data.forEach(function (daemon: any) {
      counts[daemon.health] = (counts[daemon.health] || 0) + 1;
    });
    data.forEach(function (daemon: any) {
      healthColorMap[daemon.health] = daemon.health_color;
    });
    let result = [];
    for (let prop in counts) {
      if (counts.hasOwnProperty(prop)) {
        result.push({ health: prop, count: counts[prop], health_color: healthColorMap[prop] });
      }
    }
    console.log(result);
    return result;
  }

  navigateToServices() {
    this.router.navigate(['/services/']);
  }
  ngOnDestroy(): void {
    this.subs.unsubscribe();
  }

  refresh() {
    this.rbdMirroringService.refresh();
  }
}

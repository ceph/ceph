import { Component, OnDestroy, OnInit, TemplateRef, ViewChild } from '@angular/core';

import { Subscription } from 'rxjs';

import { RbdMirroringService } from '~/app/shared/api/rbd-mirroring.service';
import { TableStatusViewCache } from '~/app/shared/classes/table-status-view-cache';

@Component({
  selector: 'cd-mirroring-daemons',
  templateUrl: './daemon-list.component.html',
  styleUrls: ['./daemon-list.component.scss']
})
export class DaemonListComponent implements OnInit, OnDestroy {
  @ViewChild('healthTmpl', { static: true })
  healthTmpl: TemplateRef<any>;

  subs: Subscription;

  data: any[];
  daemons: {};
  empty: boolean;

  tableStatus = new TableStatusViewCache();

  constructor(private rbdMirroringService: RbdMirroringService) {
    this.empty = false;
  }

  ngOnInit() {
    this.subs = this.rbdMirroringService.subscribeSummary((data) => {
      this.data = data.content_data.daemons;
      this.tableStatus = new TableStatusViewCache(data.status);
      this.daemons = this.countDaemons();
    });
  }

  ngOnDestroy(): void {
    this.subs.unsubscribe();
  }

  countDaemons(): {} {
    const daemon_states = { info: 0, error: 0, warning: 0, success: 0 };
    this.empty = this.data.length > 0 ? false : true;
    for (let i = 0; i < this.data.length; i++) {
      const health_color = this.data[i]['health_color'];
      daemon_states[health_color]++;
    }
    return daemon_states;
  }

  daemonStatus(daemon_status: string): string {
    const display_names = {success: 'UP', error: 'DOWN', warning: 'WARNING', info: 'UNKNOWN'};
    return display_names[daemon_status];
  }
}

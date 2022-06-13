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
    const daemon_states = { unknown: 0, error: 0, warning: 0, success: 0 };
    this.empty = this.data.length > 0 ? false : true;
    for (let i = 0; i < this.data.length; i++) {
      const daemon_data = this.data[i];
      if (daemon_data['health'] === 'OK') {
        daemon_states.success++;
      } else if (daemon_data['health'] === 'Error') {
        daemon_states.error++;
      } else if (daemon_data['health'] === 'Warning') {
        daemon_states.warning++;
      } else {
        daemon_states.unknown++;
      }
    }
    return daemon_states;
  }

  daemonStatus(daemon_status: string): string {
    if (daemon_status === 'success') {
      return 'UP';
    } else if (daemon_status === 'error') {
      return 'DOWN';
    } else if (daemon_status === 'warning') {
      return 'WARNING';
    } else {
      return 'UNKNOWN';
    }
  }
}

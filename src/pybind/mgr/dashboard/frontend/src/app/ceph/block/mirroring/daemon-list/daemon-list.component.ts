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

  data: [];
  daemons: {};
  empty: boolean;

  tableStatus = new TableStatusViewCache();

  constructor(
    private rbdMirroringService: RbdMirroringService,
  ) {
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
    let n = {unknown: 0, error: 0, warning: 0, success: 0};
    this.empty = (this.data.length > 0) ? false : true; 
    for (let i = 0; i < this.data.length; i++) {
      let d = this.data[i];
      if(d['health'] == 'OK')
        n.success++;
      else if (d['health'] == 'Error')
        n.error++;
      else if (d['health'] == 'Warning')
        n.warning++;
      else
        n.unknown++;
    }
    return n;
  }

  daemonStatus(s: string): string {
    if (s === 'success')
      return 'UP';
    else if (s === 'error')
      return 'DOWN';
    else if (s === 'warning')
      return 'WARNING';
    else
      return 'UNKNOWN';
  }

}

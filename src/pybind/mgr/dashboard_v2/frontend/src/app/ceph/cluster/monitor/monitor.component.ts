import { Component } from '@angular/core';

import { CellTemplate } from '../../../shared/enum/cell-template.enum';
import { MonitorService } from '../monitor.service';

@Component({
  selector: 'cd-monitor',
  templateUrl: './monitor.component.html',
  styleUrls: ['./monitor.component.scss']
})
export class MonitorComponent {

  mon_status: any;
  inQuorum: any;
  notInQuorum: any;

  interval: any;
  sparklineStyle = {
    height: '30px',
    width: '50%'
  };

  constructor(private monitorService: MonitorService) {
    this.inQuorum = {
      columns: [
        { prop: 'name', name: 'Name', cellTransformation: CellTemplate.routerLink },
        { prop: 'rank', name: 'Rank' },
        { prop: 'public_addr', name: 'Public Address' },
        {
          prop: 'cdOpenSessions',
          name: 'Open Sessions',
          cellTransformation: CellTemplate.sparkline
        }
      ],
      data: []
    };

    this.notInQuorum = {
      columns: [
        { prop: 'name', name: 'Name', cellTransformation: CellTemplate.routerLink },
        { prop: 'rank', name: 'Rank' },
        { prop: 'public_addr', name: 'Public Address' }
      ],
      data: []
    };
  }

  refresh() {
    this.monitorService.getMonitor().subscribe((data: any) => {
      data.in_quorum.map((row) => {
        row.cdOpenSessions = row.stats.num_sessions.map(i => i[1]);
        row.cdLink = '/perf_counters/mon/' + row.name;
        return row;
      });

      data.out_quorum.map((row) => {
        row.cdLink = '/perf_counters/mon/' + row.name;
        return row;
      });

      this.inQuorum.data = [...data.in_quorum];
      this.notInQuorum.data = [...data.out_quorum];
      this.mon_status = data.mon_status;
    });
  }
}

import { Component, OnInit, TemplateRef, ViewChild } from '@angular/core';

import _ from 'lodash';

import { MonitorService } from '~/app/shared/api/monitor.service';
import { CellTemplate } from '~/app/shared/enum/cell-template.enum';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';

const enum QuorumPresent {
  Yes = 'Yes',
  No = 'No'
}

interface PublicAddressObject {
  type: string;
  addr: string;
}

interface QuorumData {
  columns: CdTableColumn[];
  data?: any[];
}

@Component({
  selector: 'cd-monitor',
  templateUrl: './monitor.component.html',
  styleUrls: ['./monitor.component.scss'],
  standalone: false
})
export class MonitorComponent implements OnInit {
  mon_status: any;
  quorum: QuorumData = { columns: [], data: [] };
  interval: any;
  title: string = $localize`Monitors`;
  description: string = $localize`Maintains the master copy of the cluster state, including the monitor map, OSD map, and CRUSH map`;

  @ViewChild('publicAddressesTpl', { static: true })
  publicAddressesTpl!: TemplateRef<any>;

  protected static publicAddressArray(public_addrs_object: {
    addrvec: PublicAddressObject[];
  }): string[] {
    if (!public_addrs_object?.addrvec) {
      return [];
    }

    return public_addrs_object.addrvec
      .filter((addr: PublicAddressObject) => ['v1', 'v2'].includes(addr.type))
      .sort((addr1: PublicAddressObject, addr2: PublicAddressObject) => {
        return addr1.type.localeCompare(addr2.type);
      })
      .map((addr: PublicAddressObject) => {
        return `${addr.type}: ${addr.addr}`;
      });
  }

  constructor(private monitorService: MonitorService) {}

  ngOnInit(): void {
    this.quorum = {
      columns: [
        { prop: 'name', name: $localize`Name`, cellTransformation: CellTemplate.routerLink },
        { prop: 'rank', name: $localize`Rank` },
        {
          prop: 'public_addr',
          name: $localize`Public address`,
          cellTemplate: this.publicAddressesTpl
        },
        {
          prop: 'status',
          name: $localize`In Quorum`,
          cellTransformation: CellTemplate.tag,
          customTemplateConfig: {
            map: {
              Yes: { value: $localize`Yes`, class: 'tag-success' },
              No: { value: $localize`No`, class: 'tag-danger' }
            }
          }
        },
        {
          prop: 'cdOpenSessions',
          name: $localize`Open sessions`,
          cellTransformation: CellTemplate.sparkline,
          comparator: (dataA: any, dataB: any) => {
            // We get the last value of time series to compare:
            const lastValueA = _.last(dataA);
            const lastValueB = _.last(dataB);

            if (!lastValueA || !lastValueB || lastValueA === lastValueB) {
              return 0;
            }

            return lastValueA > lastValueB ? 1 : -1;
          }
        }
      ]
    };
  }

  refresh() {
    this.monitorService.getMonitor().subscribe((data: any) => {
      data.in_quorum.map((row: any) => {
        row.cdOpenSessions = row.stats.num_sessions.map((i: string) => i[1]);
        row.cdLink = '/perf_counters/mon/' + row.name;
        row.cdParams = { fromLink: '/monitor' };
        row.status = QuorumPresent.Yes;
        row.public_addr = MonitorComponent.publicAddressArray(row.public_addrs);
        return row;
      });

      data.out_quorum.map((row: any) => {
        row.cdLink = '/perf_counters/mon/' + row.name;
        row.cdParams = { fromLink: '/monitor' };
        row.status = QuorumPresent.No;
        row.cdOpenSessions = [];
        row.public_addr = MonitorComponent.publicAddressArray(row.public_addrs);
        return row;
      });

      this.quorum.data = [...data.in_quorum, ...data.out_quorum];
      this.mon_status = data.mon_status;
    });
  }
}

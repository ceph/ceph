import { Pipe, PipeTransform } from '@angular/core';

import _ from 'lodash';
import { Host, HostList } from '../models/host-schema';

@Pipe({
  name: 'hostSummary'
})
export class HostSummaryPipe implements PipeTransform {
  transform(hosts: HostList): any {
    if (!hosts) {
      return '';
    }

    let successCount = 0;
    let warnCount = 0;
    let errorCount = 0;
    _.each(hosts, (host: Host) => {
      if (!host.status) {
        successCount++;
      } else if (host.status === 'maintenance') {
        warnCount++;
      } else {
        errorCount++;
      }
    });
    const hostSummary = {
      total: hosts.length,
      success: successCount,
      warn: warnCount,
      error: errorCount
    };
    return hostSummary;
  }
}

import { NgZone } from '@angular/core';

import { TableStatus } from './table-status';

export class ListWithDetails {
  expandedRow: any;
  staleTimeout: number;
  tableStatus: TableStatus;

  constructor(protected ngZone?: NgZone) {}

  setExpandedRow(expandedRow: any) {
    this.expandedRow = expandedRow;
  }

  setTableRefreshTimeout() {
    clearTimeout(this.staleTimeout);
    this.ngZone.runOutsideAngular(() => {
      this.staleTimeout = window.setTimeout(() => {
        this.ngZone.run(() => {
          this.tableStatus = new TableStatus(
            'secondary',
            $localize`The user list data might be stale. If needed, you can manually reload it.`
          );
        });
      }, 10000);
    });
  }
}

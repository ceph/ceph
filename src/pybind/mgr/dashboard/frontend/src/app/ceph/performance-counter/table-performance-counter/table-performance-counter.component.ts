import { Component, Input, OnInit, TemplateRef, ViewChild } from '@angular/core';

import { I18n } from '@ngx-translate/i18n-polyfill';

import { PerformanceCounterService } from '../../../shared/api/performance-counter.service';
import { CdTableColumn } from '../../../shared/models/cd-table-column';
import { CdTableFetchDataContext } from '../../../shared/models/cd-table-fetch-data-context';

/**
 * Display the specified performance counters in a datatable.
 */
@Component({
  selector: 'cd-table-performance-counter',
  templateUrl: './table-performance-counter.component.html',
  styleUrls: ['./table-performance-counter.component.scss']
})
export class TablePerformanceCounterComponent implements OnInit {
  columns: Array<CdTableColumn> = [];
  counters: Array<object> = [];

  @ViewChild('valueTpl', { static: false })
  public valueTpl: TemplateRef<any>;

  /**
   * The service type, e.g. 'rgw', 'mds', 'mon', 'osd', ...
   */
  @Input()
  serviceType: string;

  /**
   * The service identifier.
   */
  @Input()
  serviceId: string;

  constructor(private performanceCounterService: PerformanceCounterService, private i18n: I18n) {}

  ngOnInit() {
    this.columns = [
      {
        name: this.i18n('Name'),
        prop: 'name',
        flexGrow: 1
      },
      {
        name: this.i18n('Description'),
        prop: 'description',
        flexGrow: 1
      },
      {
        name: this.i18n('Value'),
        prop: 'value',
        cellTemplate: this.valueTpl,
        flexGrow: 1
      }
    ];
  }

  getCounters(context: CdTableFetchDataContext) {
    this.performanceCounterService.get(this.serviceType, this.serviceId).subscribe(
      (resp: object[]) => {
        this.counters = resp;
      },
      (error) => {
        if (error.status === 404) {
          error.preventDefault();
          this.counters = null;
        } else {
          context.error();
        }
      }
    );
  }
}

import { Component, Input, OnInit, TemplateRef, ViewChild } from '@angular/core';

import { CdTableColumn } from '../../../shared/models/cd-table-column';
import { TablePerformanceCounterService } from '../services/table-performance-counter.service';

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

  @ViewChild('valueTpl') public valueTpl: TemplateRef<any>;

  /**
   * The service type, e.g. 'rgw', 'mds', 'mon', 'osd', ...
   */
  @Input() serviceType: string;

  /**
   * The service identifier.
   */
  @Input() serviceId: string;

  constructor(private performanceCounterService: TablePerformanceCounterService) { }

  ngOnInit() {
    this.columns = [
      {
        name: 'Name',
        prop: 'name',
        flexGrow: 1
      },
      {
        name: 'Description',
        prop: 'description',
        flexGrow: 1
      },
      {
        name: 'Value',
        cellTemplate: this.valueTpl,
        flexGrow: 1
      }
    ];
  }

  getCounters() {
    this.performanceCounterService.get(this.serviceType, this.serviceId)
      .then((resp) => {
        this.counters = resp;
      });
  }
}

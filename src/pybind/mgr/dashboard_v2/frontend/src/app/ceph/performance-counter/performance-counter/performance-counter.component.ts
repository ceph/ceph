import { Component, OnDestroy, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';

import { TablePerformanceCounterService } from '../services/table-performance-counter.service';

@Component({
  selector: 'cd-performance-counter',
  templateUrl: './performance-counter.component.html',
  styleUrls: ['./performance-counter.component.scss']
})
export class PerformanceCounterComponent implements OnInit, OnDestroy {
  serviceId: string;
  serviceType: string;
  routeParamsSubscribe: any;

  constructor(
    private route: ActivatedRoute,
    private performanceCounterService: TablePerformanceCounterService
  ) {}

  ngOnInit() {
    this.routeParamsSubscribe = this.route.params.subscribe(
      (params: { type: string; id: string }) => {
        this.serviceId = params.id;
        this.serviceType = params.type;
      }
    );
  }

  ngOnDestroy() {
    this.routeParamsSubscribe.unsubscribe();
  }
}

import { Component, OnDestroy } from '@angular/core';
import { ActivatedRoute } from '@angular/router';

@Component({
  selector: 'cd-performance-counter',
  templateUrl: './performance-counter.component.html',
  styleUrls: ['./performance-counter.component.scss']
})
export class PerformanceCounterComponent implements OnDestroy {
  serviceId: string;
  serviceType: string;
  routeParamsSubscribe: any;

  constructor(private route: ActivatedRoute) {
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

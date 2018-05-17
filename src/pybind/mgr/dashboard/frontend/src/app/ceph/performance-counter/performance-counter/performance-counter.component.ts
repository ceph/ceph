import { Component } from '@angular/core';
import { ActivatedRoute } from '@angular/router';

@Component({
  selector: 'cd-performance-counter',
  templateUrl: './performance-counter.component.html',
  styleUrls: ['./performance-counter.component.scss']
})
export class PerformanceCounterComponent {

  static defaultFromLink = '/hosts';

  serviceId: string;
  serviceType: string;
  fromLink: string;

  constructor(private route: ActivatedRoute) {
    this.route.queryParams.subscribe(
      (params: { fromLink: string }) => {
        this.fromLink = params.fromLink || PerformanceCounterComponent.defaultFromLink;
      }
    );
    this.route.params.subscribe(
      (params: { type: string; id: string }) => {
        this.serviceId = params.id;
        this.serviceType = params.type;
      }
    );
  }

}

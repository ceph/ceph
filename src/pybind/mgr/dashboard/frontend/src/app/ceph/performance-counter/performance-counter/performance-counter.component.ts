import { Component } from '@angular/core';
import { ActivatedRoute } from '@angular/router';

@Component({
  selector: 'cd-performance-counter',
  templateUrl: './performance-counter.component.html',
  styleUrls: ['./performance-counter.component.scss']
})
export class PerformanceCounterComponent {
  serviceId: string;
  serviceType: string;

  constructor(private route: ActivatedRoute) {
    this.route.params.subscribe(
      (params: { type: string; id: string }) => {
        this.serviceId = params.id;
        this.serviceType = params.type;
      }
    );
  }

}

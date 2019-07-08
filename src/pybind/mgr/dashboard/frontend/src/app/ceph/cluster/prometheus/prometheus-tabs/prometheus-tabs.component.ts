import { Component } from '@angular/core';
import { Router } from '@angular/router';

@Component({
  selector: 'cd-prometheus-tabs',
  templateUrl: './prometheus-tabs.component.html',
  styleUrls: ['./prometheus-tabs.component.scss']
})
export class PrometheusTabsComponent {
  url: string;

  constructor(private router: Router) {
    this.url = this.router.url;
  }

  navigateTo(url) {
    this.router.navigate([url]);
  }
}

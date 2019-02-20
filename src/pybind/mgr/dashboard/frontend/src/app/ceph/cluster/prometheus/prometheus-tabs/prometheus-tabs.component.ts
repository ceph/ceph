import { Component, OnInit } from '@angular/core';
import { Router } from '@angular/router';

@Component({
  selector: 'cd-prometheus-tabs',
  templateUrl: './prometheus-tabs.component.html',
  styleUrls: ['./prometheus-tabs.component.scss']
})
export class PrometheusTabsComponent implements OnInit {
  url: string;

  constructor(private router: Router) {}

  ngOnInit() {
    this.url = this.router.url;
  }

  navigateTo(url) {
    this.router.navigate([url]);
  }
}

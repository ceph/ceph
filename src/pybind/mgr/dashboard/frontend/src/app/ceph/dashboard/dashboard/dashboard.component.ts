import { Component } from '@angular/core';
import { Title } from '@angular/platform-browser';

@Component({
  selector: 'cd-dashboard',
  templateUrl: './dashboard.component.html',
  styleUrls: ['./dashboard.component.scss']
})
export class DashboardComponent {
  hasGrafana = false; // TODO: Temporary var, remove when grafana is implemented
  constructor(private titleService: Title) {
    this.titleService.setTitle('Ceph Dashboard');
  }
}

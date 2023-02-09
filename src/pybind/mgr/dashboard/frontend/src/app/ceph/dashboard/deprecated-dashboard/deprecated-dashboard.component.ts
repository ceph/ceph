import { Component } from '@angular/core';

@Component({
  selector: 'cd-deprecated-dashboard',
  templateUrl: './deprecated-dashboard.component.html',
  styleUrls: ['./deprecated-dashboard.component.scss']
})
export class DeprecatedDashboardComponent {
  hasGrafana = false; // TODO: Temporary var, remove when grafana is implemented
  showOldPage = true;

  toggleContent() {
    this.showOldPage = !this.showOldPage;
  }
}

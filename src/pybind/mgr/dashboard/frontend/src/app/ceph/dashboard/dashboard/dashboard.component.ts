import { Component, inject, OnInit } from '@angular/core';
import { FeatureTogglesService } from '~/app/shared/services/feature-toggles.service';

@Component({
  selector: 'cd-dashboard',
  templateUrl: './dashboard.component.html',
  styleUrls: ['./dashboard.component.scss'],
  standalone: false
})
export class DashboardComponent implements OnInit {
  useDeprecated: boolean = true;

  private featureToggles = inject(FeatureTogglesService);

  ngOnInit() {
    this.useDeprecated = this.featureToggles.isFeatureEnabled('dashboard');
  }
}

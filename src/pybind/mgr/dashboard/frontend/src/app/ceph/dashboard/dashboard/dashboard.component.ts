import { Component } from '@angular/core';
import { Observable } from 'rxjs';
import { FeatureTogglesService } from '~/app/shared/services/feature-toggles.service';

@Component({
  selector: 'cd-dashboard',
  templateUrl: './dashboard.component.html',
  styleUrls: ['./dashboard.component.scss']
})
export class DashboardComponent {
  enabledFeature$: Observable<Object>;

  constructor(private featureToggles: FeatureTogglesService) {
    this.enabledFeature$ = this.featureToggles.get();
  }
}

import { Component, inject, OnInit } from '@angular/core';
import { Observable } from 'rxjs';
import {
  FeatureTogglesMap,
  FeatureTogglesService
} from '~/app/shared/services/feature-toggles.service';

@Component({
  selector: 'cd-dashboard',
  templateUrl: './dashboard.component.html',
  styleUrls: ['./dashboard.component.scss'],
  standalone: false
})
export class DashboardComponent implements OnInit {
  enabledFeature$: Observable<FeatureTogglesMap>;

  private featureToggles = inject(FeatureTogglesService);

  ngOnInit() {
    this.enabledFeature$ = this.featureToggles.get();
  }
}

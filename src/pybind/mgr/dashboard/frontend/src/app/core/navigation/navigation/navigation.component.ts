import { Component, HostBinding, OnDestroy, OnInit } from '@angular/core';

import { Subscription } from 'rxjs';

import { Icons } from '../../../shared/enum/icons.enum';
import { Permissions } from '../../../shared/models/permissions';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';
import {
  FeatureTogglesMap$,
  FeatureTogglesService
} from '../../../shared/services/feature-toggles.service';
import { PrometheusAlertService } from '../../../shared/services/prometheus-alert.service';
import { SummaryService } from '../../../shared/services/summary.service';

@Component({
  selector: 'cd-navigation',
  templateUrl: './navigation.component.html',
  styleUrls: ['./navigation.component.scss']
})
export class NavigationComponent implements OnInit, OnDestroy {
  @HostBinding('class.isPwdDisplayed') isPwdDisplayed = false;

  permissions: Permissions;
  enabledFeature$: FeatureTogglesMap$;
  summaryData: any;
  icons = Icons;

  isCollapsed = true;
  showMenuSidebar = true;
  displayedSubMenu = '';

  simplebar = {
    autoHide: false
  };
  private subs = new Subscription();

  constructor(
    private authStorageService: AuthStorageService,
    private summaryService: SummaryService,
    private featureToggles: FeatureTogglesService,
    public prometheusAlertService: PrometheusAlertService
  ) {
    this.permissions = this.authStorageService.getPermissions();
    this.enabledFeature$ = this.featureToggles.get();
  }

  ngOnInit() {
    this.subs.add(
      this.summaryService.subscribe((summary) => {
        this.summaryData = summary;
      })
    );
    this.subs.add(
      this.authStorageService.isPwdDisplayed$.subscribe((isDisplayed) => {
        this.isPwdDisplayed = isDisplayed;
      })
    );
  }

  ngOnDestroy(): void {
    this.subs.unsubscribe();
  }

  blockHealthColor() {
    if (this.summaryData && this.summaryData.rbd_mirroring) {
      if (this.summaryData.rbd_mirroring.errors > 0) {
        return { color: '#d9534f' };
      } else if (this.summaryData.rbd_mirroring.warnings > 0) {
        return { color: '#f0ad4e' };
      }
    }

    return undefined;
  }

  toggleSubMenu(menu: string) {
    if (this.displayedSubMenu === menu) {
      this.displayedSubMenu = '';
    } else {
      this.displayedSubMenu = menu;
    }
  }
}

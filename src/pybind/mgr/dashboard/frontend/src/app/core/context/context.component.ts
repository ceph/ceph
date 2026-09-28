import { AsyncPipe } from '@angular/common';
import { Component, Input, OnDestroy, OnInit } from '@angular/core';
import { Event, NavigationEnd, Router } from '@angular/router';

import { NgbDropdownModule } from '@ng-bootstrap/ng-bootstrap';
import { TagModule } from 'carbon-components-angular';
import { NEVER, Subscription } from 'rxjs';
import { filter } from 'rxjs/operators';

import { RgwDaemon } from '~/app/ceph/rgw/models/rgw-daemon';
import { RgwDaemonService } from '~/app/shared/api/rgw-daemon.service';
import { Permissions } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import {
  FeatureTogglesMap$,
  FeatureTogglesService
} from '~/app/shared/services/feature-toggles.service';
import { TimerService } from '~/app/shared/services/timer.service';

@Component({
  selector: 'cd-context',
  templateUrl: './context.component.html',
  styleUrls: ['./context.component.scss'],
  standalone: true,
  imports: [AsyncPipe, NgbDropdownModule, TagModule]
})
export class ContextComponent implements OnInit, OnDestroy {
  readonly REFRESH_INTERVAL = 5000;
  private subs = new Subscription();
  private rgwUrlPrefix = '/rgw';
  private rgwUserUrlPrefix = '/rgw/user';
  private rgwBuckerUrlPrefix = '/rgw/bucket';
  private rgwAccountsUrlPrefix = '/rgw/accounts';
  private rgwAccountsResourcePagePattern = /^\/rgw\/accounts\/[^/]+\/(overview|roles)(?:$|[?#])/;
  private rgwMultisiteSyncPolicyPrefix = '/rgw/multisite/sync-policy';
  // private rgwOverviewUrlPrefix = '/rgw/overview';
  permissions: Permissions;
  featureToggleMap$: FeatureTogglesMap$;
  isRgwResourcePage = this.isRgwAccountsResourcePage(this.getRoutePath(document.location.href));
  isRgwRoute =
    document.location.href.includes(this.rgwUserUrlPrefix) ||
    document.location.href.includes(this.rgwBuckerUrlPrefix) ||
    document.location.href.includes(this.rgwAccountsUrlPrefix) ||
    document.location.href.includes(this.rgwMultisiteSyncPolicyPrefix);

  /**
   * When true, show the context bar even if the current route is not an RGW
   * route (e.g. embed on /rgw/overview without enabling the workbench instance).
   */
  @Input() forceShow = false;

  constructor(
    private authStorageService: AuthStorageService,
    private featureToggles: FeatureTogglesService,
    private router: Router,
    private timerService: TimerService,
    public rgwDaemonService: RgwDaemonService
  ) {}

  ngOnInit() {
    this.permissions = this.authStorageService.getPermissions();
    this.featureToggleMap$ = this.featureToggles.get();
    // Check if route belongs to RGW:
    this.subs.add(
      this.router.events
        .pipe(filter((event: Event) => event instanceof NavigationEnd))
        .subscribe(() => {
          const currentRoute = this.getRoutePath(this.router.url);
          this.isRgwRoute = [
            this.rgwBuckerUrlPrefix,
            this.rgwUserUrlPrefix,
            this.rgwAccountsUrlPrefix,
            this.rgwMultisiteSyncPolicyPrefix
          ].some((urlPrefix) => this.router.url.startsWith(urlPrefix));
          this.isRgwResourcePage = this.isRgwAccountsResourcePage(currentRoute);
        })
    );
    // Set daemon list polling only when in RGW route or forced:
    this.subs.add(
      this.timerService
        .get(
          () => (this.isRgwRoute || this.forceShow ? this.rgwDaemonService.list() : NEVER),
          this.REFRESH_INTERVAL
        )
        .subscribe()
    );
  }

  ngOnDestroy() {
    this.subs.unsubscribe();
  }

  onDaemonSelection(daemon: RgwDaemon) {
    this.rgwDaemonService.selectDaemon(daemon);
    // Embedded overview context refreshes via selectedDaemon$ without remounting.
    if (!this.forceShow) {
      this.reloadData();
    }
  }

  private reloadData() {
    const currentUrl = this.router.url;
    this.router.navigateByUrl(this.rgwUrlPrefix, { skipLocationChange: true }).finally(() => {
      this.router.navigate([currentUrl]);
    });
  }

  private isRgwAccountsResourcePage(url: string): boolean {
    return this.rgwAccountsResourcePagePattern.test(url);
  }

  private getRoutePath(url: string): string {
    const hashIndex = url.indexOf('#');
    return hashIndex === -1 ? url : url.slice(hashIndex + 1);
  }
}

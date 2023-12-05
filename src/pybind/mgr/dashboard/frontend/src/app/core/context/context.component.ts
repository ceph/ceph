import { Component, OnDestroy, OnInit } from '@angular/core';
import { Event, NavigationEnd, Router } from '@angular/router';

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
  styleUrls: ['./context.component.scss']
})
export class ContextComponent implements OnInit, OnDestroy {
  readonly REFRESH_INTERVAL = 5000;
  private subs = new Subscription();
  private rgwUrlPrefix = '/rgw';
  private rgwUserUrlPrefix = '/rgw/user';
  private rgwRoleUrlPrefix = '/rgw/roles';
  private rgwBuckerUrlPrefix = '/rgw/bucket';
  permissions: Permissions;
  featureToggleMap$: FeatureTogglesMap$;
  isRgwRoute =
    document.location.href.includes(this.rgwUserUrlPrefix) ||
    document.location.href.includes(this.rgwBuckerUrlPrefix) ||
    document.location.href.includes(this.rgwRoleUrlPrefix);

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
        .subscribe(
          () =>
            (this.isRgwRoute = [
              this.rgwBuckerUrlPrefix,
              this.rgwUserUrlPrefix,
              this.rgwRoleUrlPrefix
            ].some((urlPrefix) => this.router.url.startsWith(urlPrefix)))
        )
    );
    // Set daemon list polling only when in RGW route:
    this.subs.add(
      this.timerService
        .get(() => (this.isRgwRoute ? this.rgwDaemonService.list() : NEVER), this.REFRESH_INTERVAL)
        .subscribe()
    );
  }

  ngOnDestroy() {
    this.subs.unsubscribe();
  }

  onDaemonSelection(daemon: RgwDaemon) {
    this.rgwDaemonService.selectDaemon(daemon);
    this.reloadData();
  }

  private reloadData() {
    const currentUrl = this.router.url;
    this.router.navigateByUrl(this.rgwUrlPrefix, { skipLocationChange: true }).finally(() => {
      this.router.navigate([currentUrl]);
    });
  }
}

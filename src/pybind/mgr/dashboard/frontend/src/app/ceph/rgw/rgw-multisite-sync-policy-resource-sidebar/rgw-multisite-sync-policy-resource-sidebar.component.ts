import { Component, OnDestroy, OnInit } from '@angular/core';
import { ActivatedRoute, ParamMap } from '@angular/router';
import { Subscription, combineLatest } from 'rxjs';
import { SidebarItem } from '~/app/shared/components/sidebar-layout/sidebar-layout.component';

@Component({
  selector: 'cd-rgw-multisite-sync-policy-resource-sidebar',
  templateUrl: './rgw-multisite-sync-policy-resource-sidebar.component.html',
  styleUrls: ['./rgw-multisite-sync-policy-resource-sidebar.component.scss'],
  standalone: false
})
export class RgwMultisiteSyncPolicyResourceSidebarComponent implements OnInit, OnDestroy {
  private sub = new Subscription();

  readonly basePath = '/rgw/multisite/sync-policy';
  groupNameRoute = '';
  bucketNameRoute = '';
  groupName = '';
  sidebarItems: SidebarItem[] = [];

  constructor(private route: ActivatedRoute) {}

  ngOnInit(): void {
    this.sub.add(
      combineLatest([this.route.paramMap, this.route.queryParamMap]).subscribe(
        ([pm, queryPm]: [ParamMap, ParamMap]) => {
          this.groupNameRoute = pm.get('groupName') ?? '';
          this.bucketNameRoute = queryPm.get('bucketName') ?? '';
          this.groupName = this.groupNameRoute;
          this.buildSidebarItems();
        }
      )
    );
  }

  ngOnDestroy(): void {
    this.sub.unsubscribe();
  }

  private buildSidebarItems(): void {
    const routePrefix = [this.basePath, this.groupNameRoute];
    const routeExtras = this.bucketNameRoute
      ? { queryParams: { bucketName: this.bucketNameRoute } }
      : {};

    this.sidebarItems = [
      {
        label: $localize`Overview`,
        route: [...routePrefix, 'overview'],
        routeExtras,
        routerLinkActiveOptions: { exact: true }
      },
      {
        label: $localize`Symmetrical Flows`,
        route: [...routePrefix, 'symmetrical-flows'],
        routeExtras,
        routerLinkActiveOptions: { exact: true }
      },
      {
        label: $localize`Directional Flows`,
        route: [...routePrefix, 'directional-flows'],
        routeExtras,
        routerLinkActiveOptions: { exact: true }
      },
      {
        label: $localize`Pipe`,
        route: [...routePrefix, 'pipe'],
        routeExtras,
        routerLinkActiveOptions: { exact: true }
      }
    ];
  }
}

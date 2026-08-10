import { Component, OnDestroy, OnInit, ViewEncapsulation } from '@angular/core';
import { ActivatedRoute, ParamMap } from '@angular/router';

import { Subscription } from 'rxjs';
import { SidebarItem } from '~/app/shared/components/sidebar-layout/sidebar-layout.component';

@Component({
  selector: 'cd-rgw-bucket-resource-sidebar',
  templateUrl: './rgw-bucket-resource-sidebar.component.html',
  styleUrls: ['./rgw-bucket-resource-sidebar.component.scss'],
  encapsulation: ViewEncapsulation.None,
  standalone: false
})
export class RgwBucketResourceSidebarComponent implements OnInit, OnDestroy {
  private sub = new Subscription();

  readonly basePath = '/rgw/bucket';
  bid = '';
  owner = '';
  sidebarItems: SidebarItem[] = [];

  constructor(private route: ActivatedRoute) {}

  ngOnInit(): void {
    this.sub.add(
      this.route.paramMap.subscribe((pm: ParamMap) => {
        this.bid = pm.get('bid') ?? '';
        this.owner = pm.get('owner') ?? '';

        if (!this.bid) {
          this.sidebarItems = [];
          return;
        }
        this.buildSidebarItems();
      })
    );
  }

  ngOnDestroy(): void {
    this.sub.unsubscribe();
  }

  private buildSidebarItems(): void {
    const routePrefix = [this.basePath, this.bid, this.owner];
    this.sidebarItems = [
      {
        label: $localize`Configuration`,
        route: [...routePrefix, 'configuration'],
        routerLinkActiveOptions: { exact: true }
      },
      {
        label: $localize`Permissions`,
        route: [...routePrefix, 'permissions'],
        routerLinkActiveOptions: { exact: true }
      },
      {
        label: $localize`Data management`,
        route: [...routePrefix, 'data-management'],
        routerLinkActiveOptions: { exact: true }
      },
      {
        label: $localize`Notifications`,
        route: [...routePrefix, 'notifications'],
        routerLinkActiveOptions: { exact: true }
      }
    ];
  }
}

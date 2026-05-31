import { Component, OnDestroy, OnInit, ViewEncapsulation } from '@angular/core';
import { ActivatedRoute, ParamMap } from '@angular/router';

import { Subscription } from 'rxjs';

import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { SidebarItem } from '~/app/shared/components/sidebar-layout/sidebar-layout.component';

@Component({
  selector: 'cd-pool-details',
  templateUrl: './pool-details.component.html',
  styleUrls: ['./pool-details.component.scss'],
  encapsulation: ViewEncapsulation.None,
  standalone: false
})
export class PoolDetailsComponent implements OnInit, OnDestroy {
  private sub = new Subscription();
  public readonly basePath = '/pool/view';
  poolName = '';
  sidebarItems: SidebarItem[] = [];

  constructor(private route: ActivatedRoute, private authStorageService: AuthStorageService) {}

  ngOnInit(): void {
    const permissions = this.authStorageService.getPermissions();
    this.sub.add(
      this.route.paramMap.subscribe((pm: ParamMap) => {
        this.poolName = pm.get('name') ?? '';
        this.buildSidebarItems(permissions);
      })
    );
  }

  ngOnDestroy(): void {
    this.sub.unsubscribe();
  }

  private buildSidebarItems(permissions: any): void {
    const items: SidebarItem[] = [
      {
        label: $localize`Details`,
        route: [this.basePath, this.poolName, 'details'],
        routerLinkActiveOptions: { exact: true }
      }
    ];

    if (permissions.grafana?.read) {
      items.push({
        label: $localize`Performance Details`,
        route: [this.basePath, this.poolName, 'performance-details'],
        routerLinkActiveOptions: { exact: true }
      });
    }

    items.push(
      {
        label: $localize`Configuration`,
        route: [this.basePath, this.poolName, 'configuration'],
        routerLinkActiveOptions: { exact: true }
      },
      {
        label: $localize`Cache Tiers Details`,
        route: [this.basePath, this.poolName, 'cache-tiers-details'],
        routerLinkActiveOptions: { exact: true }
      }
    );

    this.sidebarItems = items;
  }
}

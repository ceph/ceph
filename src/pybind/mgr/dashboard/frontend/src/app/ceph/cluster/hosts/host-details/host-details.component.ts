import { Component, OnDestroy, OnInit, ViewEncapsulation } from '@angular/core';
import { ActivatedRoute, ParamMap } from '@angular/router';

import { Subscription } from 'rxjs';

import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { SidebarItem } from '~/app/shared/components/sidebar-layout/sidebar-layout.component';

@Component({
  selector: 'cd-host-details',
  templateUrl: './host-details.component.html',
  styleUrls: ['./host-details.component.scss'],
  encapsulation: ViewEncapsulation.None,
  standalone: false
})
export class HostDetailsComponent implements OnInit, OnDestroy {
  private sub = new Subscription();
  public readonly basePath = '/hosts';
  hostname = '';
  sidebarItems: SidebarItem[] = [];

  constructor(private route: ActivatedRoute, private authStorageService: AuthStorageService) {}

  ngOnInit(): void {
    const permissions = this.authStorageService.getPermissions();
    this.sub.add(
      this.route.paramMap.subscribe((pm: ParamMap) => {
        this.hostname = pm.get('hostname') ?? '';
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
        label: $localize`Devices`,
        route: [this.basePath, this.hostname, 'devices'],
        routerLinkActiveOptions: { exact: true }
      }
    ];

    if (permissions.hosts?.read) {
      items.push(
        {
          label: $localize`Physical Disks`,
          route: [this.basePath, this.hostname, 'physical-disks'],
          routerLinkActiveOptions: { exact: true }
        },
        {
          label: $localize`Daemons`,
          route: [this.basePath, this.hostname, 'daemons'],
          routerLinkActiveOptions: { exact: true }
        }
      );
    }

    if (permissions.grafana?.read) {
      items.push({
        label: $localize`Performance Details`,
        route: [this.basePath, this.hostname, 'performance-details'],
        routerLinkActiveOptions: { exact: true }
      });
    }

    items.push({
      label: $localize`Device health`,
      route: [this.basePath, this.hostname, 'device-health'],
      routerLinkActiveOptions: { exact: true }
    });

    this.sidebarItems = items;
  }
}

import { Component, OnDestroy, OnInit } from '@angular/core';
import { ActivatedRoute, ParamMap } from '@angular/router';
import { Subscription } from 'rxjs';

import { RgwDaemon } from '~/app/ceph/rgw/models/rgw-daemon';
import { SidebarItem } from '~/app/shared/components/sidebar-layout/sidebar-layout.component';

@Component({
  selector: 'cd-rgw-daemon-resource-sidebar',
  templateUrl: './rgw-daemon-resource-sidebar.component.html',
  styleUrls: ['./rgw-daemon-resource-sidebar.component.scss'],
  standalone: false
})
export class RgwDaemonResourceSidebarComponent implements OnInit, OnDestroy {
  private sub = new Subscription();

  readonly basePath = '/rgw/daemon';
  daemonIdRoute = '';
  daemonName = '';
  sidebarItems: SidebarItem[] = [];

  constructor(private route: ActivatedRoute) {}

  ngOnInit(): void {
    this.sub.add(
      this.route.paramMap.subscribe((pm: ParamMap) => {
        this.daemonIdRoute = pm.get('daemonId') ?? '';
        this.buildSidebarItems();
      })
    );

    this.sub.add(
      this.route.data.subscribe((data) => {
        const daemon = (data?.daemon ?? null) as RgwDaemon | null;
        this.daemonName = daemon?.server_hostname || daemon?.id || this.daemonIdRoute;
      })
    );
  }

  ngOnDestroy(): void {
    this.sub.unsubscribe();
  }

  private buildSidebarItems(): void {
    this.sidebarItems = [
      {
        label: $localize`Overview`,
        route: [this.basePath, this.daemonIdRoute, 'overview'],
        routerLinkActiveOptions: { exact: true }
      },
      {
        label: $localize`Performance`,
        route: [this.basePath, this.daemonIdRoute, 'performance'],
        routerLinkActiveOptions: { exact: true }
      }
    ];
  }
}

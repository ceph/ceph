import { Component, OnDestroy, OnInit } from '@angular/core';
import { ActivatedRoute, ParamMap } from '@angular/router';

import { Subscription } from 'rxjs';

import { SidebarItem } from '~/app/shared/components/sidebar-layout/sidebar-layout.component';
import { RgwUser } from '../models/rgw-user';

@Component({
  selector: 'cd-rgw-user-resource-sidebar',
  templateUrl: './rgw-user-resource-sidebar.component.html',
  styleUrls: ['./rgw-user-resource-sidebar.component.scss'],
  standalone: false
})
export class RgwUserResourceSidebarComponent implements OnInit, OnDestroy {
  private sub = new Subscription();

  uid = '';
  user: RgwUser;
  sidebarItems: SidebarItem[] = [];
  readonly basePath = '/rgw/user';

  constructor(private route: ActivatedRoute) {}

  ngOnInit() {
    this.sub.add(
      this.route.paramMap.subscribe((pm: ParamMap) => {
        this.uid = pm.get('uid') ?? '';
        this.buildSidebarItems();
      })
    );

    this.sub.add(
      this.route.data.subscribe((data) => {
        this.user = data?.user ?? null;
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
        route: [this.basePath, this.uid, 'overview'],
        routerLinkActiveOptions: { exact: true }
      }
    ];
  }
}

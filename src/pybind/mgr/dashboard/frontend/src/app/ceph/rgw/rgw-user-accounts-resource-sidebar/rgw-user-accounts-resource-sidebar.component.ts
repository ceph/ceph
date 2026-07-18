import { Component, OnDestroy, OnInit } from '@angular/core';
import { ActivatedRoute, ParamMap } from '@angular/router';
import { Subscription } from 'rxjs';
import { SidebarItem } from '~/app/shared/components/sidebar-layout/sidebar-layout.component';
import { Account } from '../models/rgw-user-accounts';

@Component({
  selector: 'cd-rgw-user-accounts-resource-sidebar',
  templateUrl: './rgw-user-accounts-resource-sidebar.component.html',
  styleUrls: ['./rgw-user-accounts-resource-sidebar.component.scss'],
  standalone: false
})
export class RgwUserAccountsResourceSidebarComponent implements OnInit, OnDestroy {
  private sub = new Subscription();
  readonly basePath = '/rgw/accounts';
  accountNameRoute = '';
  accountId = '';
  accountName = '';
  sidebarItems: SidebarItem[] = [];

  constructor(private route: ActivatedRoute) {}

  ngOnInit(): void {
    this.sub.add(
      this.route.paramMap.subscribe((pm: ParamMap) => {
        this.accountNameRoute = pm.get('accountName') ?? '';
        this.buildSidebarItems();
      })
    );

    this.sub.add(
      this.route.data.subscribe((data) => {
        const account = (data?.account ?? null) as Account | null;
        this.accountId = account?.id ?? '';
        this.accountName = account?.name ?? this.accountNameRoute;
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
        route: [this.basePath, this.accountNameRoute, 'overview'],
        routerLinkActiveOptions: { exact: true }
      },
      {
        label: $localize`Roles`,
        route: [this.basePath, this.accountNameRoute, 'roles'],
        routerLinkActiveOptions: { exact: true }
      }
    ];
  }
}

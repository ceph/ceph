import { Component, OnDestroy, OnInit } from '@angular/core';
import { ActivatedRoute, ParamMap, Router } from '@angular/router';
import { Subscription } from 'rxjs';
import { SidebarItem } from '~/app/shared/components/sidebar-layout/sidebar-layout.component';
import { getUsersGroupsPath } from '../utils';

@Component({
  selector: 'cd-smb-usersgroups-resource-sidebar',
  templateUrl: './smb-usersgroups-resource-sidebar.component.html',
  styleUrls: ['./smb-usersgroups-resource-sidebar.component.scss'],
  standalone: false
})
export class SmbUsersgroupsResourceSidebarComponent implements OnInit, OnDestroy {
  private sub = new Subscription();
  usersGroupsIdRoute = '';
  standaloneName = '';
  sidebarItems: SidebarItem[] = [];

  constructor(private route: ActivatedRoute, private router: Router) {}

  ngOnInit() {
    this.sub.add(
      this.route.paramMap.subscribe((pm: ParamMap) => {
        this.usersGroupsIdRoute = pm.get('users_groups_id') ?? '';
        this.buildSidebarItems();
        this.loadTitle();
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
        route: [`/${getUsersGroupsPath(this.router.url)}`, this.usersGroupsIdRoute, 'overview'],
        routerLinkActiveOptions: { exact: true }
      }
    ];
  }

  private loadTitle(): void {
    this.standaloneName = this.usersGroupsIdRoute;
  }
}

import { Component, OnInit } from '@angular/core';
import { Router } from '@angular/router';

import { Permission } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';

const RBD_PATH = 'block/rbd';

enum TABS {
  images = 'images',
  namespaces = 'namespaces',
  trash = 'trash',
  performance = 'performance'
}

@Component({
  selector: 'cd-rbd-tabs',
  templateUrl: './rbd-tabs.component.html',
  styleUrls: ['./rbd-tabs.component.scss'],
  standalone: false
})
export class RbdTabsComponent implements OnInit {
  grafanaPermission: Permission;
  selectedTab: TABS;
  activeTab: TABS = TABS.images;

  constructor(private authStorageService: AuthStorageService, private router: Router) {
    this.grafanaPermission = this.authStorageService.getPermissions().grafana;
  }

  ngOnInit(): void {
    const currentPath = this.router.url;
    this.activeTab = Object.values(TABS).find((t) => currentPath.includes(t)) || TABS.images;
  }

  onSelected(tab: TABS) {
    this.selectedTab = tab;
    const route = tab === TABS.images ? RBD_PATH : `${RBD_PATH}/${tab}`;
    this.router.navigate([route]);
  }

  public get Tabs(): typeof TABS {
    return TABS;
  }
}

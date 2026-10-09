import { Component, OnInit } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';

import { resolveSmbRouteData } from '../smb-route.util';

enum TABS {
  cluster = 'cluster',
  activeDirectory = 'active-directory',
  standalone = 'standalone',
  overview = 'overview'
}

@Component({
  selector: 'cd-smb-tabs',
  templateUrl: './smb-tabs.component.html',
  styleUrls: ['./smb-tabs.component.scss'],
  standalone: false
})
export class SmbTabsComponent implements OnInit {
  selectedTab: TABS;
  activeTab: TABS = TABS.cluster;
  private readonly smbBasePath: string;

  constructor(
    private router: Router,
    private route: ActivatedRoute
  ) {
    this.smbBasePath = resolveSmbRouteData(this.route).smbBasePath;
  }

  ngOnInit(): void {
    const currentPath = this.router.url;
    this.activeTab = Object.values(TABS).find((tab) => currentPath.includes(tab)) || TABS.cluster;
  }

  onSelected(tab: TABS) {
    this.selectedTab = tab;
    this.router.navigate([`${this.smbBasePath}/${tab}`]);
  }

  public get Tabs(): typeof TABS {
    return TABS;
  }
}

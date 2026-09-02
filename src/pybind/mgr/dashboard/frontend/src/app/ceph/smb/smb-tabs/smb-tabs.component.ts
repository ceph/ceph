import { Component, inject, OnInit } from '@angular/core';
import { Router } from '@angular/router';

import { SMB_CONTEXT } from '~/app/shared/constants/smb.constant';

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
  private readonly ctx = inject(SMB_CONTEXT);
  private readonly router = inject(Router);

  ngOnInit(): void {
    const currentPath = this.router.url;
    this.activeTab = Object.values(TABS).find((tab) => currentPath.includes(tab)) || TABS.cluster;
  }

  onSelected(tab: TABS) {
    this.selectedTab = tab;
    this.router.navigate([`${this.ctx.basePath}/${tab}`]);
  }

  public get Tabs(): typeof TABS {
    return TABS;
  }
}

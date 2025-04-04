import { Component, OnInit } from '@angular/core';
import { Router } from '@angular/router';

enum TABS {
  clusters = 'clusters',
  activeDirectory = 'active-directory',
  standalone = 'standalone',
  overview = 'overview'
}

@Component({
  selector: 'cd-smb-tabs',
  templateUrl: './smb-tabs.component.html',
  styleUrls: ['./smb-tabs.component.scss']
})
export class SmbTabsComponent implements OnInit {
  selectedTab: TABS;
  activeTab: TABS = TABS.clusters;

  constructor(private router: Router) {}

  ngOnInit(): void {
    const currentPath = this.router.url;
    this.activeTab = Object.values(TABS).find((tab) => currentPath.includes(tab)) || TABS.clusters;
  }

  onSelected(tab: TABS) {
    this.selectedTab = tab;
    this.router.navigate([`/cephfs/smb/${tab}`]);
  }

  public get Tabs(): typeof TABS {
    return TABS;
  }
}

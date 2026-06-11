import { Component, OnInit } from '@angular/core';
import { Router } from '@angular/router';

const MULTISITE_PATH = 'rgw/multisite';

enum TABS {
  configuration = 'configuration',
  syncPolicy = 'sync-policy'
}

@Component({
  selector: 'cd-rgw-multisite-tabs',
  templateUrl: './rgw-multisite-tabs.component.html',
  styleUrls: ['./rgw-multisite-tabs.component.scss'],
  standalone: false
})
export class RgwMultisiteTabsComponent implements OnInit {
  selectedTab: TABS;
  activeTab: TABS = TABS.configuration;

  constructor(private router: Router) {}

  ngOnInit(): void {
    const currentPath = this.router.url;
    this.activeTab = Object.values(TABS).find((t) => currentPath.includes(t)) || TABS.configuration;
  }

  onSelected(tab: TABS) {
    this.selectedTab = tab;
    this.router.navigate([`${MULTISITE_PATH}/${tab}`]);
  }

  public get Tabs(): typeof TABS {
    return TABS;
  }
}

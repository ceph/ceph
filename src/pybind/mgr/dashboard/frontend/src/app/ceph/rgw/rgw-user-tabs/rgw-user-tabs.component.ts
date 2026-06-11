import { Component, OnInit } from '@angular/core';
import { Router } from '@angular/router';

const RGW_PATH = 'rgw';

enum TABS {
  user = 'user',
  accounts = 'accounts',
  roles = 'roles'
}

@Component({
  selector: 'cd-rgw-user-tabs',
  templateUrl: './rgw-user-tabs.component.html',
  styleUrls: ['./rgw-user-tabs.component.scss'],
  standalone: false
})
export class RgwUserTabsComponent implements OnInit {
  selectedTab: TABS;
  activeTab: TABS = TABS.user;

  constructor(private router: Router) {}

  ngOnInit(): void {
    const currentPath = this.router.url;
    this.activeTab = Object.values(TABS).find((t) => currentPath.includes(t)) || TABS.user;
  }

  onSelected(tab: TABS) {
    this.selectedTab = tab;
    this.router.navigate([`${RGW_PATH}/${tab}`]);
  }

  public get Tabs(): typeof TABS {
    return TABS;
  }
}

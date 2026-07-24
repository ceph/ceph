import { Component, OnInit } from '@angular/core';
import { Router } from '@angular/router';

const USER_MGMT_PATH = 'user-management';

enum TABS {
  users = 'users',
  roles = 'roles'
}

@Component({
  selector: 'cd-user-tabs',
  templateUrl: './user-tabs.component.html',
  styleUrls: ['./user-tabs.component.scss'],
  standalone: false
})
export class UserTabsComponent implements OnInit {
  selectedTab: TABS;
  activeTab: TABS = TABS.users;

  constructor(private router: Router) {}

  ngOnInit(): void {
    const currentPath = this.router.url;
    this.activeTab = Object.values(TABS).find((t) => currentPath.includes(t)) || TABS.users;
  }

  onSelected(tab: TABS) {
    this.selectedTab = tab;
    this.router.navigate([`${USER_MGMT_PATH}/${tab}`]);
  }

  public get Tabs(): typeof TABS {
    return TABS;
  }
}

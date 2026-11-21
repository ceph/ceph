import { Component, OnInit } from '@angular/core';
import { Router } from '@angular/router';

enum TABS {
  users = 'users',
  roles = 'roles'
}

const BASE_PATH = '/user-management';

@Component({
  selector: 'cd-user-tabs',
  templateUrl: './user-tabs.component.html',
  styleUrls: ['./user-tabs.component.scss'],
  standalone: false
})
export class UserTabsComponent implements OnInit {
  activeTab: TABS = TABS.users;

  constructor(public router: Router) {}

  ngOnInit(): void {
    this.activeTab = this.router.url.includes(`${BASE_PATH}/${TABS.roles}`)
      ? TABS.roles
      : TABS.users;
  }

  onSelected(tab: TABS) {
    // Carbon emits `selected` for the initially active tab as well, so only navigate on change.
    if (tab === this.activeTab) {
      return;
    }
    this.activeTab = tab;
    this.router.navigate([`${BASE_PATH}/${tab}`]);
  }

  public get Tabs(): typeof TABS {
    return TABS;
  }
}

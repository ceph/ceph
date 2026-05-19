import { Component, OnInit } from '@angular/core';
import { Router } from '@angular/router';

const ISCSI_PATH = 'block/iscsi';

enum TABS {
  overview = 'overview',
  targets = 'targets'
}

@Component({
  selector: 'cd-iscsi-tabs',
  templateUrl: './iscsi-tabs.component.html',
  styleUrls: ['./iscsi-tabs.component.scss'],
  standalone: false
})
export class IscsiTabsComponent implements OnInit {
  selectedTab: TABS;
  activeTab: TABS = TABS.overview;

  constructor(private router: Router) {}

  ngOnInit(): void {
    const currentPath = this.router.url;
    this.activeTab = Object.values(TABS).find((t) => currentPath.includes(t)) || TABS.overview;
  }

  onSelected(tab: TABS) {
    this.selectedTab = tab;
    this.router.navigate([`${ISCSI_PATH}/${tab}`]);
  }

  public get Tabs(): typeof TABS {
    return TABS;
  }
}

import { Component } from '@angular/core';

enum TABS {
  'clusters',
  'joinAuths',
  'usersgroups'
}

@Component({
  selector: 'cd-smb-tabs',
  templateUrl: './smb-tabs.component.html',
  styleUrls: ['./smb-tabs.component.scss']
})
export class SmbTabsComponent {
  selectedTab: TABS;

  onSelected(tab: TABS) {
    this.selectedTab = tab;
  }

  public get Tabs(): typeof TABS {
    return TABS;
  }
}

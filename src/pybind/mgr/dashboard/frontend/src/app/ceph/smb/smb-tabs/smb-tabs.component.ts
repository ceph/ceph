import { Component, OnInit } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';

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

  constructor(
    private router: Router,
    private route: ActivatedRoute
  ) {}

  ngOnInit(): void {
    const currentPath = this.router.url;
    this.activeTab = Object.values(TABS).find((tab) => currentPath.includes(tab)) || TABS.cluster;
  }

  onSelected(tab: TABS) {
    this.selectedTab = tab;
    this.router.navigate([`${this.route.snapshot.data['smbBasePath'] ?? 'cephfs/smb'}/${tab}`]);
  }

  public get Tabs(): typeof TABS {
    return TABS;
  }
}

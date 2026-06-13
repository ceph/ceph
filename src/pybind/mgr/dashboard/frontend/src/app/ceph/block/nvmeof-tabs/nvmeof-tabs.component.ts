import { Component, OnInit } from '@angular/core';
import { Router } from '@angular/router';

const NVMEOF_PATH = 'block/nvmeof';

enum TABS {
  gateways = 'gateways',
  subsystems = 'subsystems',
  namespaces = 'namespaces'
}

@Component({
  selector: 'cd-nvmeof-tabs',
  templateUrl: './nvmeof-tabs.component.html',
  styleUrls: ['./nvmeof-tabs.component.scss'],
  standalone: false
})
export class NvmeofTabsComponent implements OnInit {
  selectedTab: TABS;
  activeTab: TABS = TABS.gateways;

  constructor(private router: Router) {}

  ngOnInit(): void {
    const currentPath = this.router.url;
    this.activeTab = Object.values(TABS).find((tab) => currentPath.includes(tab)) || TABS.gateways;
  }

  onSelected(tab: TABS) {
    this.selectedTab = tab;
    this.router.navigate([`${NVMEOF_PATH}/${tab}`]);
  }

  public get Tabs(): typeof TABS {
    return TABS;
  }
}

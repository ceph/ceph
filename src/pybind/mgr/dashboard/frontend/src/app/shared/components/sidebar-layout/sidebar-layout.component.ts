import { Component, Input, ViewEncapsulation } from '@angular/core';

export interface SidebarItem {
  label: string;
  route: any[];
  routerLinkActiveOptions?: { exact: boolean };
}

@Component({
  selector: 'cd-sidebar-layout',
  templateUrl: './sidebar-layout.component.html',
  styleUrls: ['./sidebar-layout.component.scss'],
  encapsulation: ViewEncapsulation.None,
  standalone: false
})
export class SidebarLayoutComponent {
  @Input() title!: string;
  @Input() items: SidebarItem[] = [];

  constructor() {}
}

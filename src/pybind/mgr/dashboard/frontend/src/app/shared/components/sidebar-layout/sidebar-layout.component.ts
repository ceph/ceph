import { Component, Input, ViewEncapsulation } from '@angular/core';

export interface SidebarItem {
  label: string;
  route: string[];
  routerLinkActiveOptions?: { exact: boolean };
}

@Component({
  selector: 'cd-sidebar-layout',
  templateUrl: './sidebar-layout.component.html',
  styleUrls: ['./sidebar-layout.component.scss'],
  encapsulation: ViewEncapsulation.None,
  standalone: false,
  host: {
    class: 'tearsheet--full'
  }
})
export class SidebarLayoutComponent {
  @Input() title!: string;
  @Input() items: SidebarItem[] = [];
}

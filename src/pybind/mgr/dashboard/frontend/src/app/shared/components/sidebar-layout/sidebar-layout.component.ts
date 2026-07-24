import { Component, Input, ViewEncapsulation } from '@angular/core';
import {
  ResourceHeaderAction,
  ResourceHeaderStatus
} from '../page-header-resource/page-header-resource.component';

export interface SidebarItem {
  label: string;
  route: string[];
  routeExtras?: any;
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
  @Input() headerStatus?: ResourceHeaderStatus;
  @Input() headerTags: string[] = [];
  @Input() headerActions: ResourceHeaderAction[] = [];
  @Input() showHeaderBreadcrumbs = true;
}

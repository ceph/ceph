import { ChangeDetectionStrategy, Component, Input } from '@angular/core';

import { IBadgeConfig } from '../../../shared/components/badge/badge.component';
import { Permissions } from '../../../shared/models/permissions';

export interface IMenuItem {
  label: string;
  link?: string;
  children?: IMenuItem[];
  permissions?: keyof Permissions;
  badges?: IBadgeConfig[];
}

@Component({
  selector: 'cd-menu',
  templateUrl: 'menu.component.html',
  styleUrls: ['menu.component.scss'],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class MenuComponent {
  @Input() items: IMenuItem[];
  childExpanded: IMenuItem;
}

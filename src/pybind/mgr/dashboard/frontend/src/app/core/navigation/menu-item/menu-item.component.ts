import { ChangeDetectionStrategy, Component, Input } from '@angular/core';

import { IMenuItem } from '../menu/menu.component';

@Component({
  selector: 'menu-item',
  templateUrl: 'menu-item.component.html',
  styleUrls: ['menu-item.component.scss'],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class MenuItemComponent {
  @Input() item: IMenuItem;
}

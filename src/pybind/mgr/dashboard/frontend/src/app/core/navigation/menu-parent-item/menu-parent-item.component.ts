import {
  ChangeDetectionStrategy,
  ChangeDetectorRef,
  Component,
  EventEmitter,
  Input,
  Output,
  ViewChild
} from '@angular/core';

import { MenuItemComponent } from '../menu-item/menu-item.component';
import { IMenuItem } from '../menu/menu.component';

@Component({
  selector: 'menu-parent-item',
  templateUrl: 'menu-parent-item.component.html',
  styleUrls: ['menu-parent-item.component.scss'],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class MenuParentItemComponent {
  @Input() item: IMenuItem;
  @Input() expanded = false;
  @Output() clicked: EventEmitter<IMenuItem> = new EventEmitter();

  noSubitems: boolean;
  @ViewChild(MenuItemComponent, { static: false }) set subitem(item: boolean) {
    this.noSubitems = item === undefined;
    this.cdr.detectChanges();
  }

  constructor(private cdr: ChangeDetectorRef) {}
}

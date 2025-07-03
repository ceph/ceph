import { Component, Input } from '@angular/core';
import { ICON_TYPE, Icons, IconsSize } from '../../enum/icons.enum';

@Component({
  selector: 'cd-icon',
  templateUrl: './icon.component.html',
  styleUrl: './icon.component.scss'
})
export class IconComponent {
  @Input() type!: keyof typeof ICON_TYPE;
  @Input() size: IconsSize = IconsSize.size16;

  readonly ICONS = Icons;
  readonly ICON_TYPE = ICON_TYPE;
}

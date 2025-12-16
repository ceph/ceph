import { Component, Input, OnInit, ViewEncapsulation } from '@angular/core';
import { ICON_TYPE, Icons, IconSize } from '../../enum/icons.enum';

@Component({
  selector: 'cd-icon',
  templateUrl: './icon.component.html',
  styleUrl: './icon.component.scss',
  standalone: false,
  encapsulation: ViewEncapsulation.None
})
export class IconComponent implements OnInit {
  @Input() type!: keyof typeof ICON_TYPE;
  @Input() size: IconSize = IconSize.size16;
  @Input() class: string = '';
  // No CSS class will be applied.
  @Input() useDefault: boolean = false;

  icon: string;

  ngOnInit() {
    this.icon = ICON_TYPE[this.type];
  }
}

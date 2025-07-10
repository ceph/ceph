import { Component, Input, OnInit } from '@angular/core';
import { ICON_TYPE, Icons, IconSize } from '../../enum/icons.enum';

@Component({
  selector: 'cd-icon',
  templateUrl: './icon.component.html',
  styleUrl: './icon.component.scss'
})
export class IconComponent implements OnInit {
  @Input() type!: keyof typeof ICON_TYPE;
  @Input() size: IconSize = IconSize.size16;

  icon: string;

  ngOnInit() {
    this.icon = Icons[this.type];
  }
}

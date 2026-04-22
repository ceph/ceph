import {
  Component,
  Input,
  OnChanges,
  OnInit,
  SimpleChanges
} from '@angular/core';
import { ICON_TYPE, IconSize } from '../../enum/icons.enum';

@Component({
  selector: 'cd-icon',
  templateUrl: './icon.component.html',
  styleUrl: './icon.component.scss'
})
export class IconComponent implements OnInit, OnChanges {
  @Input() type!: keyof typeof ICON_TYPE;
  @Input() size: IconSize = IconSize.size16;
  @Input() class: string = '';

  icon: string;

  ngOnInit() {
    this.updateIcon();
  }

  ngOnChanges(changes: SimpleChanges) {
    if (changes['type']) {
      this.updateIcon();
    }
  }

  private updateIcon() {
    this.icon = ICON_TYPE[this.type];
  }
}

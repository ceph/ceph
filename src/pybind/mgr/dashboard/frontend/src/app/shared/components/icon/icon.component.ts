import {
  Component,
  Input,
  OnChanges,
  OnInit,
  SimpleChanges,
  ViewEncapsulation
} from '@angular/core';
import { ICON_TYPE, Icons, IconSize } from '../../enum/icons.enum';

@Component({
  selector: 'cd-icon',
  templateUrl: './icon.component.html',
  styleUrl: './icon.component.scss',
  standalone: false,
  encapsulation: ViewEncapsulation.None
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
    this.icon = Icons[this.type];
  }
}

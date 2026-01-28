import { Component, Input } from '@angular/core';
import { IconSize } from '~/app/shared/enum/icons.enum';

@Component({
  selector: 'cd-card',
  templateUrl: './card.component.html',
  styleUrls: ['./card.component.scss'],
  standalone: false
})
export class CardComponent {
  iconSize = IconSize;

  @Input()
  cardTitle: string;
  @Input()
  cardType: string = '';
  @Input()
  removeBorder = false;
  @Input()
  shadow = false;
  @Input()
  cardFooter = false;
  @Input()
  fullHeight = false;
  @Input()
  alignItemsCenter = false;
  @Input()
  justifyContentCenter = false;
}

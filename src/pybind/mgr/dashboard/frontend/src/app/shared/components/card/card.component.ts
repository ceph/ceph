import { Component, Input } from '@angular/core';
import { Icons } from '~/app/shared/enum/icons.enum';

@Component({
  selector: 'cd-card',
  templateUrl: './card.component.html',
  styleUrls: ['./card.component.scss']
})
export class CardComponent {
  icons = Icons;

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

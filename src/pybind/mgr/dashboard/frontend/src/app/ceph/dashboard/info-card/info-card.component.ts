import { Component, Input } from '@angular/core';

@Component({
  selector: 'cd-info-card',
  templateUrl: './info-card.component.html',
  styleUrls: ['./info-card.component.scss'],
  standalone: false
})
export class InfoCardComponent {
  @Input()
  cardTitle: string;
  @Input()
  link: string;
  @Input()
  cardClass = '';
  @Input()
  contentClass: string;
}

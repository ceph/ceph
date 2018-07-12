import { Component, Input } from '@angular/core';

@Component({
  selector: 'cd-info-card',
  templateUrl: './info-card.component.html',
  styleUrls: ['./info-card.component.scss']
})
export class InfoCardComponent {
  @Input() title: string;
  @Input() link: string;
  @Input() cardClass = 'col-md-6';
  @Input() imageClass: string;
  @Input() contentClass: string;
}

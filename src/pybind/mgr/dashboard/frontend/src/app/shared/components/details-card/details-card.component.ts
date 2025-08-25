import { Component, Input } from '@angular/core';

@Component({
  selector: 'cd-details-card',
  templateUrl: './details-card.component.html',
  styleUrl: './details-card.component.scss'
})
export class DetailsCardComponent {
  @Input()
  cardTitle: string;
}

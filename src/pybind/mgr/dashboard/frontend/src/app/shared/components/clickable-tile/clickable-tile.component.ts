import { Component, EventEmitter, Input, Output } from '@angular/core';

@Component({
  selector: 'cd-clickable-tile',
  templateUrl: './clickable-tile.component.html',
  standalone: false
})
export class ClickableTileComponent {
  @Input({ required: true }) title!: string;
  @Input({ required: true }) description!: string;
  @Input({ required: true }) icon!: string;

  @Output() tileClick = new EventEmitter<void>();
}

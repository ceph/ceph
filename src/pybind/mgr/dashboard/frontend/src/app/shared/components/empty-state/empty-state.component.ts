import { Component, Input } from '@angular/core';
import { EMPTY_STATE_IMAGE } from '../../enum/icons.enum';

@Component({
  selector: 'cd-empty-state',
  standalone: true,
  templateUrl: './empty-state.component.html',
  styleUrl: './empty-state.component.scss'
})
export class EmptyStateComponent {
  @Input() text: string | null = '';
  @Input() title: string | null = '';
  @Input() imgSrc: string = EMPTY_STATE_IMAGE.default;
}

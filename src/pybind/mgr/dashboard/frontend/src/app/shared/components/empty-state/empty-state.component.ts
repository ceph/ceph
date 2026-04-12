import { Component, Input } from '@angular/core';

@Component({
  selector: 'cd-empty-state',
  standalone: true,
  templateUrl: './empty-state.component.html',
  styleUrl: './empty-state.component.scss'
})
export class EmptyStateComponent {
  /* Optional: Custom empty state text, when empty state is displyed*/
  @Input() emptyStateText: string | null = '';
}

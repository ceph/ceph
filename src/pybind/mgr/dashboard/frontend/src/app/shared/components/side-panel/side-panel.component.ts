import { Component, EventEmitter, Input, Output } from '@angular/core';

@Component({
  selector: 'cd-side-panel',
  templateUrl: './side-panel.component.html',
  styleUrl: './side-panel.component.scss',
  standalone: false
})
export class SidePanelComponent {
  @Input() expanded = false;
  @Input() headerText = '';
  @Input() overlay = true;
  @Input() size: 'sm' | 'md' | 'lg' | 'xl' | '2xl' | 'full' = 'lg';
  @Input() layer: number;
  @Input() theme: string;
  @Input() showCloseButton = true;
  // Disable the default content padding when the projected content manages its own layout.
  @Input() contentPadding = true;

  @Output() closed = new EventEmitter<void>();

  close() {
    this.closed.emit();
  }
}

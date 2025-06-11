import { Component, EventEmitter, Input, Output } from '@angular/core';

@Component({
  selector: 'cd-side-panel',
  templateUrl: './side-panel.component.html',
  styleUrl: './side-panel.component.scss'
})
export class SidePanelComponent {
  @Input() expanded = false;
  @Input() headerText = '';
  @Input() overlay = true;
  @Input() size: 'sm' | 'md' | 'lg' = 'lg';

  @Output() closed = new EventEmitter<void>();

  close() {
    this.closed.emit();
  }
}

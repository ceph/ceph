import { ChangeDetectionStrategy, Component, EventEmitter, Input, Output } from '@angular/core';

@Component({
  selector: 'cd-form-section-toggle',
  templateUrl: './form-section-toggle.component.html',
  styleUrls: ['./form-section-toggle.component.scss'],
  changeDetection: ChangeDetectionStrategy.OnPush,
  standalone: false
})
export class FormSectionToggleComponent {
  @Input()
  expanded = false;
  @Input()
  showText: string;
  @Input()
  hideText: string;

  @Output()
  expandedChange = new EventEmitter<boolean>();

  toggle(event: Event) {
    event.preventDefault();
    this.expanded = !this.expanded;
    this.expandedChange.emit(this.expanded);
  }
}

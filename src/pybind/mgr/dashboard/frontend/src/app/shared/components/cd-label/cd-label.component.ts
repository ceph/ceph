import { Component, Input } from '@angular/core';

@Component({
  selector: 'cd-label',
  templateUrl: './cd-label.component.html',
  styleUrls: ['./cd-label.component.scss'],
  standalone: false
})
export class CdLabelComponent {
  @Input() key?: string;
  @Input() value?: string;
  @Input() tooltipText?: string;
}

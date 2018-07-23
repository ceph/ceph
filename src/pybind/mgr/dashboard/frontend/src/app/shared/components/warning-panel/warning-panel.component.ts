import { Component, Input } from '@angular/core';

@Component({
  selector: 'cd-warning-panel',
  templateUrl: './warning-panel.component.html',
  styleUrls: ['./warning-panel.component.scss']
})
export class WarningPanelComponent {
  /**
   * The title to be displayed. Defaults to 'Warning'.
   * @type {string}
   */
  @Input() title = 'Warning';
}

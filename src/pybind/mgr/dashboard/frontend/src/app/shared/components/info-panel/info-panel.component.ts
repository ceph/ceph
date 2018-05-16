import { Component, Input } from '@angular/core';

@Component({
  selector: 'cd-info-panel',
  templateUrl: './info-panel.component.html',
  styleUrls: ['./info-panel.component.scss']
})
export class InfoPanelComponent {
  /**
   * The title to be displayed. Defaults to 'Information'.
   * @type {string}
   */
  @Input() title = 'Information';
}

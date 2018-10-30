import { Component, Input } from '@angular/core';

import { I18n } from '@ngx-translate/i18n-polyfill';

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
  @Input()
  title = this.i18n('Information');

  constructor(private i18n: I18n) {}
}

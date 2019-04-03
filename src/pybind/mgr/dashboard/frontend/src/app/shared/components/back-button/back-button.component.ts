import { Location } from '@angular/common';
import { Component, Input } from '@angular/core';

import { I18n } from '@ngx-translate/i18n-polyfill';

@Component({
  selector: 'cd-back-button',
  templateUrl: './back-button.component.html',
  styleUrls: ['./back-button.component.scss']
})
export class BackButtonComponent {
  constructor(private location: Location, private i18n: I18n) {}

  @Input() name: string = this.i18n('Back');
  @Input() back: Function = () => this.location.back();
}

import { Location } from '@angular/common';
import { Component, Input } from '@angular/core';

import { ActionLabelsI18n } from '../../constants/app.constants';

@Component({
  selector: 'cd-back-button',
  templateUrl: './back-button.component.html',
  styleUrls: ['./back-button.component.scss']
})
export class BackButtonComponent {
  constructor(private location: Location, private actionLabels: ActionLabelsI18n) {}

  @Input() name: string = this.actionLabels.CANCEL;
  @Input() back: Function = () => this.location.back();
}

import { Location } from '@angular/common';
import { Component, EventEmitter, Input, Output } from '@angular/core';

import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';

@Component({
  selector: 'cd-back-button',
  templateUrl: './back-button.component.html',
  styleUrls: ['./back-button.component.scss']
})
export class BackButtonComponent {
  @Output() backAction = new EventEmitter();
  @Input() name: string = this.actionLabels.CANCEL;

  constructor(private location: Location, private actionLabels: ActionLabelsI18n) {}

  back() {
    if (this.backAction.observers.length === 0) {
      this.location.back();
    } else {
      this.backAction.emit();
    }
  }
}

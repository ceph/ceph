import { Location } from '@angular/common';
import { Component, EventEmitter, Input, OnInit, Output } from '@angular/core';

import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';

@Component({
  selector: 'cd-back-button',
  templateUrl: './back-button.component.html',
  styleUrls: ['./back-button.component.scss']
})
export class BackButtonComponent implements OnInit {
  @Output() backAction = new EventEmitter();
  @Input() name?: string;

  constructor(private location: Location, private actionLabels: ActionLabelsI18n) {}

  ngOnInit(): void {
    this.name = this.name || this.actionLabels.CANCEL;
  }

  back() {
    if (this.backAction.observers.length === 0) {
      this.location.back();
    } else {
      this.backAction.emit();
    }
  }
}

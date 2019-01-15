import { Component, Input, ViewChild } from '@angular/core';
import { ValidatorFn } from '@angular/forms';

import { I18n } from '@ngx-translate/i18n-polyfill';
import * as _ from 'lodash';

import { SelectMessages } from '../select/select-messages.model';
import { SelectOption } from '../select/select-option.model';

@Component({
  selector: 'cd-select-badges',
  templateUrl: './select-badges.component.html',
  styleUrls: ['./select-badges.component.scss']
})
export class SelectBadgesComponent {
  @Input()
  data: Array<string> = [];
  @Input()
  options: Array<SelectOption> = [];
  @Input()
  messages = new SelectMessages({}, this.i18n);
  @Input()
  selectionLimit: number;
  @Input()
  customBadges = false;
  @Input()
  customBadgeValidators: ValidatorFn[] = [];

  @ViewChild('cdSelect')
  cdSelect;

  constructor(private i18n: I18n) {}
}

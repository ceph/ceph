import { Component, EventEmitter, Input, Output, ViewChild } from '@angular/core';
import { ValidatorFn } from '@angular/forms';

import { Icons } from '../../../shared/enum/icons.enum';
import { SelectMessages } from '../select/select-messages.model';
import { SelectOption } from '../select/select-option.model';
import { SelectComponent } from '../select/select.component';

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
  messages = new SelectMessages({});
  @Input()
  selectionLimit: number;
  @Input()
  customBadges = false;
  @Input()
  customBadgeValidators: ValidatorFn[] = [];

  @Output()
  selection = new EventEmitter();

  @ViewChild('cdSelect', { static: true })
  cdSelect: SelectComponent;

  icons = Icons;
}

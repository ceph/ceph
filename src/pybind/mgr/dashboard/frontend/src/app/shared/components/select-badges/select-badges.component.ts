import { Component, Input, OnChanges, OnInit } from '@angular/core';
import { FormControl, ValidatorFn } from '@angular/forms';

import * as _ from 'lodash';

import { CdFormGroup } from '../../forms/cd-form-group';
import { SelectBadgesMessages } from './select-badges-messages.model';
import { SelectBadgesOption } from './select-badges-option.model';

@Component({
  selector: 'cd-select-badges',
  templateUrl: './select-badges.component.html',
  styleUrls: ['./select-badges.component.scss']
})
export class SelectBadgesComponent implements OnInit, OnChanges {
  @Input()
  data: Array<string> = [];
  @Input()
  options: Array<SelectBadgesOption> = [];
  @Input()
  messages = new SelectBadgesMessages({});
  @Input()
  selectionLimit: number;
  @Input()
  customBadges = false;
  @Input()
  customBadgeValidators: ValidatorFn[] = [];
  form: CdFormGroup;
  filter: FormControl;
  Object = Object;
  filteredOptions: Array<SelectBadgesOption> = [];

  constructor() {}

  ngOnInit() {
    this.initFilter();
    if (this.data.length > 0) {
      this.initMissingOptions();
    }
    this.options = _.sortBy(this.options, ['name']);
    this.updateOptions();
  }

  private initFilter() {
    this.filter = new FormControl('', { validators: this.customBadgeValidators });
    this.form = new CdFormGroup({ filter: this.filter });
    this.filteredOptions = [...this.options];
  }

  private initMissingOptions() {
    const options = this.options.map((option) => option.name);
    const needToCreate = this.data.filter((option) => options.indexOf(option) === -1);
    needToCreate.forEach((option) => this.addOption(option));
    this.forceOptionsToReflectData();
  }

  private addOption(name: string) {
    this.options.push(new SelectBadgesOption(false, name, ''));
    this.options = _.sortBy(this.options, ['name']);
    this.triggerSelection(this.options.find((option) => option.name === name));
  }

  triggerSelection(option: SelectBadgesOption) {
    if (
      !option ||
      (this.selectionLimit && !option.selected && this.data.length >= this.selectionLimit)
    ) {
      return;
    }
    option.selected = !option.selected;
    this.updateOptions();
  }

  private updateOptions() {
    this.data.splice(0, this.data.length);
    this.options.forEach((option: SelectBadgesOption) => {
      if (option.selected) {
        this.data.push(option.name);
      }
    });
    this.updateFilter();
  }

  updateFilter() {
    this.filteredOptions = this.options.filter((option) => option.name.includes(this.filter.value));
  }

  private forceOptionsToReflectData() {
    this.options.forEach((option) => {
      if (this.data.indexOf(option.name) !== -1) {
        option.selected = true;
      }
    });
  }

  ngOnChanges() {
    if (!this.options || !this.data || this.data.length === 0) {
      return;
    }
    this.forceOptionsToReflectData();
  }

  selectOption() {
    if (this.filteredOptions.length === 0) {
      this.addCustomOption();
    } else {
      this.triggerSelection(this.filteredOptions[0]);
      this.resetFilter();
    }
  }

  addCustomOption() {
    if (!this.isCreatable()) {
      return;
    }
    this.addOption(this.filter.value);
    this.resetFilter();
  }

  isCreatable() {
    return (
      this.customBadges &&
      this.filter.valid &&
      this.filter.value.length > 0 &&
      this.filteredOptions.every((option) => option.name !== this.filter.value)
    );
  }

  private resetFilter() {
    this.filter.setValue('');
    this.updateFilter();
  }

  removeItem(item: string) {
    this.triggerSelection(
      this.options.find((option: SelectBadgesOption) => option.name === item && option.selected)
    );
  }
}

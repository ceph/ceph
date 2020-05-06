import { Component, EventEmitter, Input, OnChanges, OnInit, Output } from '@angular/core';
import { FormControl, ValidatorFn } from '@angular/forms';

import * as _ from 'lodash';

import { Icons } from '../../../shared/enum/icons.enum';
import { CdFormGroup } from '../../forms/cd-form-group';
import { SelectMessages } from './select-messages.model';
import { SelectOption } from './select-option.model';

@Component({
  selector: 'cd-select',
  templateUrl: './select.component.html',
  styleUrls: ['./select.component.scss']
})
export class SelectComponent implements OnInit, OnChanges {
  @Input()
  elemClass: string;
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

  form: CdFormGroup;
  filter: FormControl;
  Object = Object;
  filteredOptions: Array<SelectOption> = [];
  icons = Icons;

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
    this.filteredOptions = [...(this.options || [])];
  }

  private initMissingOptions() {
    const options = this.options.map((option) => option.name);
    const needToCreate = this.data.filter((option) => options.indexOf(option) === -1);
    needToCreate.forEach((option) => this.addOption(option));
    this.forceOptionsToReflectData();
  }

  private addOption(name: string) {
    this.options.push(new SelectOption(false, name, ''));
    this.options = _.sortBy(this.options, ['name']);
    this.triggerSelection(this.options.find((option) => option.name === name));
  }

  triggerSelection(option: SelectOption) {
    if (
      !option ||
      (this.selectionLimit && !option.selected && this.data.length >= this.selectionLimit)
    ) {
      return;
    }
    option.selected = !option.selected;
    this.updateOptions();
    this.selection.emit({ option: option });
  }

  private updateOptions() {
    this.data.splice(0, this.data.length);
    this.options.forEach((option: SelectOption) => {
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
    if (this.filter) {
      this.updateFilter();
    }
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
      this.options.find((option: SelectOption) => option.name === item && option.selected)
    );
  }
}

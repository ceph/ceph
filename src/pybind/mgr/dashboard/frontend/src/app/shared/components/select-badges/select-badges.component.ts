import { Component, OnChanges, OnInit } from '@angular/core';
import { Input } from '@angular/core';
import { FormControl, ValidatorFn } from '@angular/forms';
import { CdFormGroup } from '../../forms/cd-form-group';
import { CdValidators } from '../../forms/cd-validators';
import { SelectBadgesOption } from './select-badges-option.model';

@Component({
  selector: 'cd-select-badges',
  templateUrl: './select-badges.component.html',
  styleUrls: ['./select-badges.component.scss']
})
export class SelectBadgesComponent implements OnInit, OnChanges {
  @Input() data: Array<string> = [];
  @Input() options: Array<SelectBadgesOption> = [];
  @Input()
  errorMessages = {
    empty: 'There are no items.',
    selectionLimit: 'Selection limit reached',
    custom: {
      validation: {},
      duplicate: 'Already exits'
    }
  };
  @Input() selectionLimit: number;
  @Input() customBadges = false;
  @Input() customBadgeValidators: ValidatorFn[] = [];
  @Input() customBadgeMessage = 'Use custom tag';
  form: CdFormGroup;
  customBadge: FormControl;
  Object = Object;

  constructor() {}

  ngOnInit() {
    if (this.customBadges) {
      this.initCustomBadges();
    }
    if (this.data.length > 0) {
      this.initMissingOptions();
    }
  }

  private initCustomBadges() {
    this.customBadgeValidators.push(
      CdValidators.custom(
        'duplicate',
        (badge) => this.options && this.options.some((option) => option.name === badge)
      )
    );
    this.customBadge = new FormControl('', { validators: this.customBadgeValidators });
    this.form = new CdFormGroup({ customBadge: this.customBadge });
  }

  private initMissingOptions() {
    const options = this.options.map((option) => option.name);
    const needToCreate = this.data.filter((option) => options.indexOf(option) === -1);
    needToCreate.forEach((option) => this.addOption(option));
    this.forceOptionsToReflectData();
  }

  private addOption(name: string) {
    this.options.push(new SelectBadgesOption(false, name, ''));
    this.triggerSelection(this.options[this.options.length - 1]);
  }

  private triggerSelection(option: SelectBadgesOption) {
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

  addCustomOption() {
    if (this.customBadge.invalid || this.customBadge.value.length === 0) {
      return;
    }
    this.addOption(this.customBadge.value);
    this.customBadge.setValue('');
  }

  removeItem(item: string) {
    this.triggerSelection(
      this.options.find((option: SelectBadgesOption) => option.name === item && option.selected)
    );
  }
}

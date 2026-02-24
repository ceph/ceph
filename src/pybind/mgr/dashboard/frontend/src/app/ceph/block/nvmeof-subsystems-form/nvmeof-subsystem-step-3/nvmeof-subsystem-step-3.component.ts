import { Component, Input, OnInit } from '@angular/core';
import { FormArray, UntypedFormControl } from '@angular/forms';

import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { AUTHENTICATION, StepTwoType } from '~/app/shared/models/nvmeof';
import { TearsheetStep } from '~/app/shared/models/tearsheet-step';

@Component({
  selector: 'cd-nvmeof-subsystem-step-three',
  templateUrl: './nvmeof-subsystem-step-3.component.html',
  styleUrls: ['./nvmeof-subsystem-step-3.component.scss'],
  standalone: false
})
export class NvmeofSubsystemsStepThreeComponent implements OnInit, TearsheetStep {
  @Input() group!: string;
  @Input() set stepTwoValue(value: StepTwoType | null) {
    this._addedHosts = value?.addedHosts ?? [];
    if (this.formGroup) {
      this.syncHostList();
    }
  }

  formGroup: CdFormGroup;
  action: string;
  pageURL: string;
  INVALID_TEXTS = {
    required: $localize`This field is required`
  };
  AUTHENTICATION = AUTHENTICATION;

  _addedHosts: Array<string> = [];

  constructor(public actionLabels: ActionLabelsI18n) {}

  private syncHostList() {
    const currentList = this.hostDchapKeyList;

    // save existing dhchap keys by host_nqn
    const existing = new Map<string, string | null>();
    currentList.getRawValue().forEach((x: any) => existing.set(x.host_nqn, x.dhchap_key));

    currentList.clear();

    const hosts = this._addedHosts;

    if (hosts.length) {
      hosts.forEach((nqn) => {
        currentList.push(this.createHostDhchapKeyFormGroup(nqn, existing.get(nqn) ?? null));
      });
    } else {
      currentList.push(this.createHostDhchapKeyFormGroup('', null));
    }
  }

  private createForm() {
    this.formGroup = new CdFormGroup({
      authType: new UntypedFormControl(AUTHENTICATION.Unidirectional),
      subsystemDchapKey: new UntypedFormControl(null),
      hostDchapKeyList: new FormArray([])
    });

    this.syncHostList();
  }

  private createHostDhchapKeyFormGroup(hostNQN: string = '', key: string | null = null) {
    return new CdFormGroup({
      dhchap_key: new UntypedFormControl(key),
      host_nqn: new UntypedFormControl(hostNQN)
    });
  }

  ngOnInit() {
    this.createForm();
  }

  get hostDchapKeyList() {
    return this.formGroup.get('hostDchapKeyList') as FormArray;
  }

  trackByIndex = (i: number) => i;
}

import { Component, Input, OnInit } from '@angular/core';
import { FormArray, UntypedFormControl } from '@angular/forms';

import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { AUTHENTICATION, HostStepType } from '~/app/shared/models/nvmeof';
import { TearsheetStep } from '~/app/shared/models/tearsheet-step';

@Component({
  selector: 'cd-nvmeof-subsystem-step-three',
  templateUrl: './nvmeof-subsystem-step-3.component.html',
  styleUrls: ['./nvmeof-subsystem-step-3.component.scss'],
  standalone: false
})
export class NvmeofSubsystemsStepThreeComponent implements OnInit, TearsheetStep {
  @Input() group!: string;
  @Input() set stepTwoValue(value: HostStepType | null) {
    this._addedHosts = value?.addedHosts ?? [];
    if (this.formGroup) {
      this.syncHostList();
    }
  }

  formGroup: CdFormGroup;
  action: string;
  pageURL: string;
  INVALID_TEXTS = {
    required: $localize`This field is required`,
    invalidBase64: $localize`Invalid key format. Use Base64 or DHHC-1:XX:base64:`
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
    }
  }

  private createForm() {
    this.formGroup = new CdFormGroup({
      authType: new UntypedFormControl(AUTHENTICATION.Unidirectional),
      subsystemDchapKey: new UntypedFormControl(null, [
        CdValidators.base64(),
        CdValidators.requiredIf({
          authType: AUTHENTICATION.Bidirectional
        })
      ]),
      hostDchapKeyList: new FormArray([])
    });

    this.syncHostList();
    this.formGroup.get('authType')?.valueChanges.subscribe(() => {
      this.refreshHostKeyValidation();
    });
  }

  private createHostDhchapKeyFormGroup(hostNQN: string = '', key: string | null = null) {
    return new CdFormGroup({
      dhchap_key: new UntypedFormControl(key, {
        validators: [
          CdValidators.base64(),
          CdValidators.custom(
            'required',
            (value: string) =>
              this.formGroup?.get('authType')?.value === AUTHENTICATION.Bidirectional && !value
          )
        ]
      }),
      host_nqn: new UntypedFormControl(hostNQN)
    });
  }

  private refreshHostKeyValidation() {
    this.hostDchapKeyList.controls.forEach((control) => {
      control.get('dhchap_key')?.updateValueAndValidity({ emitEvent: false });
    });
  }

  ngOnInit() {
    this.createForm();
  }

  get hostDchapKeyList() {
    return this.formGroup.get('hostDchapKeyList') as FormArray;
  }

  hostDhchapKeyCtrl(i: number) {
    return (this.hostDchapKeyList.at(i) as CdFormGroup).get('dhchap_key');
  }

  trackByIndex = (i: number) => i;
}

import { Component, Input, OnInit, TemplateRef, ViewChild, ViewEncapsulation } from '@angular/core';
import { FormControl, UntypedFormControl } from '@angular/forms';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';

import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { HOST_TYPE } from '~/app/shared/models/nvmeof';
import { TearsheetStep } from '~/app/shared/models/tearsheet-step';

@Component({
  selector: 'cd-nvmeof-subsystem-step-two',
  templateUrl: './nvmeof-subsystem-step-2.component.html',
  styleUrls: ['./nvmeof-subsystem-step-2.component.scss'],
  standalone: false,
  encapsulation: ViewEncapsulation.None
})
export class NvmeofSubsystemsStepTwoComponent implements OnInit, TearsheetStep {
  @Input() group!: string;
  @Input() existingHosts: string[] = [];
  @ViewChild('rightInfluencer', { static: true })
  rightInfluencer?: TemplateRef<any>;
  formGroup: CdFormGroup;
  action: string;
  pageURL: string;
  INVALID_TEXTS = {
    pattern: $localize`Expected NQN format: "nqn.$year-$month.$reverseDomainName:$utf8-string" or "nqn.2014-08.org.nvmexpress:uuid:$UUID-string"`,
    customRequired: $localize`This field is required`,
    duplicate: $localize`Duplicate entry detected. Enter a unique value.`
  };
  HOST_TYPE = HOST_TYPE;
  addedHostsLength: number = 0;
  NQN_REGEX = /^nqn\.(19|20)\d\d-(0[1-9]|1[0-2])\.\D{2,3}(\.[A-Za-z0-9-]+)+(:[A-Za-z0-9-\.]+(:[A-Za-z0-9-\.]+)*)$/;
  NQN_REGEX_UUID = /^nqn\.2014-08\.org\.nvmexpress:uuid:[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/;
  ALLOW_ALL_HOST = '*';

  constructor(public actionLabels: ActionLabelsI18n, public activeModal: NgbActiveModal) {}

  ngOnInit() {
    this.createForm();
    this.formGroup.get('hostType').valueChanges.subscribe(() => {
      this.formGroup.get('hostname').updateValueAndValidity();
    });
  }

  isValidNQN = CdValidators.custom(
    'pattern',
    (input: string) => !!input && !(this.NQN_REGEX.test(input) || this.NQN_REGEX_UUID.test(input))
  );

  isDuplicate = CdValidators.custom(
    'duplicate',
    (input: string) =>
      !!input &&
      (this.formGroup?.get('addedHosts')?.value.includes(input) ||
        this.existingHosts.includes(input))
  );

  isRequired = CdValidators.custom(
    'customRequired',
    (input: string) =>
      !input &&
      this.addedHostsLength === 0 &&
      this.formGroup?.get('hostType')?.value === this.HOST_TYPE.SPECIFIC
  );

  showRightInfluencer(): boolean {
    return this.formGroup.get('hostType')?.value === this.HOST_TYPE.SPECIFIC;
  }

  createForm() {
    this.formGroup = new CdFormGroup({
      hostType: new UntypedFormControl(this.HOST_TYPE.SPECIFIC),
      hostname: new FormControl<string>('', {
        validators: [this.isValidNQN, this.isDuplicate, this.isRequired]
      }),
      addedHosts: new FormControl<string[]>([])
    });
  }

  addHost() {
    const hostnameCtrl = this.formGroup.get('hostname');
    hostnameCtrl.markAsTouched();
    hostnameCtrl.updateValueAndValidity();
    if (hostnameCtrl.value && hostnameCtrl.valid) {
      const addedHosts = this.formGroup.get('addedHosts').value;
      const newHostList = [...addedHosts, hostnameCtrl.value];
      this.addedHostsLength = newHostList.length;
      this.formGroup.patchValue({
        addedHosts: newHostList,
        hostname: ''
      });
    }
  }

  removeHost(removedHost: string) {
    const currentAddedHosts = this.formGroup.get('addedHosts').value;
    const newHostList = currentAddedHosts.filter((currentHost) => currentHost !== removedHost);
    this.addedHostsLength = newHostList.length;
    this.formGroup.patchValue({
      addedHosts: newHostList
    });
    this.formGroup.get('hostname').updateValueAndValidity();
  }

  removeAll() {
    this.addedHostsLength = 0;
    this.formGroup.patchValue({
      addedHosts: []
    });
    this.formGroup.get('hostname').updateValueAndValidity();
  }
}

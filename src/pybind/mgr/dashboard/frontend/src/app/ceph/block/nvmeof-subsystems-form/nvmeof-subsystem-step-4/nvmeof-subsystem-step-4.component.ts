import { Component, Input, OnInit } from '@angular/core';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';

import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { AUTHENTICATION, HOST_TYPE, NO_AUTH } from '~/app/shared/models/nvmeof';
import { TearsheetStep } from '~/app/shared/models/tearsheet-step';

@Component({
  selector: 'cd-nvmeof-subsystem-step-four',
  templateUrl: './nvmeof-subsystem-step-4.component.html',
  styleUrls: ['./nvmeof-subsystem-step-4.component.scss'],
  standalone: false
})
export class NvmeofSubsystemsStepFourComponent implements OnInit, TearsheetStep {
  @Input() group!: string;
  @Input() nqn: string = '';
  @Input() listeners: any[] = [];
  @Input() hostType: string = HOST_TYPE.SPECIFIC;
  @Input() addedHosts: string[] = [];
  @Input() authType: string = AUTHENTICATION.Unidirectional;
  @Input() subsystemDchapKey: string = '';
  @Input() hostDchapKeyCount: number = 0;

  formGroup: CdFormGroup;
  HOST_TYPE = HOST_TYPE;
  AUTHENTICATION = AUTHENTICATION;

  constructor(public actionLabels: ActionLabelsI18n, public activeModal: NgbActiveModal) {}

  ngOnInit() {
    this.formGroup = new CdFormGroup({});
  }

  get listenerCount(): number {
    return this.listeners?.length || 0;
  }

  get hostAccessLabel(): string {
    return this.hostType === HOST_TYPE.ALL ? $localize`All hosts` : $localize`Restricted`;
  }

  get hostCount(): number {
    return this.addedHosts?.length || 0;
  }

  get authTypeLabel(): string {
    if (this.authType === AUTHENTICATION.None) return NO_AUTH;
    return this.authType === AUTHENTICATION.Bidirectional
      ? $localize`Bidirectional`
      : $localize`Unidirectional`;
  }

  get hasSubsystemKey(): boolean {
    return !!this.subsystemDchapKey;
  }
}

import { Component, EventEmitter, OnInit, Output } from '@angular/core';
import { Validators } from '@angular/forms';

import { I18n } from '@ngx-translate/i18n-polyfill';
import { BsModalRef } from 'ngx-bootstrap/modal';

import { ErasureCodeProfileService } from '../../../shared/api/erasure-code-profile.service';
import { ActionLabelsI18n } from '../../../shared/constants/app.constants';
import { CdFormBuilder } from '../../../shared/forms/cd-form-builder';
import { CdFormGroup } from '../../../shared/forms/cd-form-group';

@Component({
  selector: 'cd-crush-ruleset-form',
  templateUrl: './crush-ruleset-form.component.html',
  styleUrls: ['./crush-ruleset-form.component.scss']
})
export class CrushRulesetFormComponent implements OnInit {

  form: CdFormGroup;
  devices: string[] = [];

  action: string;
  resource: string;

  constructor(
    private formBuilder: CdFormBuilder,
    public bsModalRef: BsModalRef,
    private ecpService: ErasureCodeProfileService,
    private i18n: I18n,
    public actionLabels: ActionLabelsI18n
  ) {
    this.action = this.actionLabels.CREATE;
    this.resource = this.i18n('crush ruleset');
    this.createForm();
  }

  createForm() {
    this.form = this.formBuilder.group({
      name: [null, [ Validators.required] ],
      root: [null, [ Validators.required] ],
      failureDomainType: [null, [ Validators.required] ],
      deviceClass: [null, [ Validators.required] ]
    });
  }

  ngOnInit() {
    // TODO should we use a dedicated endpoint to get devices?
    this.ecpService
      .getInfo()
      .subscribe(
        ({
          devices
        }: {
          devices: string[];
        }) => {
          this.devices = devices;
        }
      );
  }

  onSubmit() {
    // TODO
  }
}

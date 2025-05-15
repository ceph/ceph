import { Component, Inject, OnInit, Optional } from '@angular/core';
import { Validators } from '@angular/forms';

import { BaseModal } from 'carbon-components-angular';

import { OsdService } from '~/app/shared/api/osd.service';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';

@Component({
  selector: 'cd-osd-reweight-modal',
  templateUrl: './osd-reweight-modal.component.html',
  styleUrls: ['./osd-reweight-modal.component.scss']
})
export class OsdReweightModalComponent extends BaseModal implements OnInit {
  reweightForm: CdFormGroup;

  constructor(
    public actionLabels: ActionLabelsI18n,
    private osdService: OsdService,
    private fb: CdFormBuilder,

    @Optional() @Inject('currentWeight') public currentWeight = 1,
    @Optional() @Inject('osdId') public osdId: number
  ) {
    super();
  }

  get weight() {
    return this.reweightForm.get('weight');
  }

  ngOnInit() {
    this.reweightForm = this.fb.group({
      weight: this.fb.control(this.currentWeight, [Validators.required])
    });
  }

  reweight() {
    this.osdService
      .reweight(this.osdId, this.reweightForm.value.weight)
      .subscribe(() => this.closeModal());
  }
}

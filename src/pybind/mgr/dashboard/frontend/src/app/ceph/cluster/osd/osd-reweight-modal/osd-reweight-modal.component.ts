import { Component, OnInit } from '@angular/core';
import { Validators } from '@angular/forms';

import * as _ from 'lodash';
import { BsModalRef } from 'ngx-bootstrap/modal';

import { OsdService } from '../../../../shared/api/osd.service';
import { CdFormBuilder } from '../../../../shared/forms/cd-form-builder';
import { CdFormGroup } from '../../../../shared/forms/cd-form-group';

@Component({
  selector: 'cd-osd-reweight-modal',
  templateUrl: './osd-reweight-modal.component.html',
  styleUrls: ['./osd-reweight-modal.component.scss']
})
export class OsdReweightModalComponent implements OnInit {
  // Input
  currentWeight = 1;
  osdId: number;
  onSubmit: Function;

  // Internal
  reweightForm: CdFormGroup;

  constructor(
    public bsModalRef: BsModalRef,
    private osdService: OsdService,
    private fb: CdFormBuilder
  ) {}

  get weight() {
    return this.reweightForm.get('weight');
  }

  ngOnInit() {
    this.reweightForm = this.fb.group({
      weight: this.fb.control(this.currentWeight, [
        Validators.required,
        Validators.max(1),
        Validators.min(0)
      ])
    });
  }

  submitAction() {
    if (_.isFunction(this.onSubmit)) {
      this.onSubmit();
    }
    this.osdService.reweight(this.osdId, this.reweightForm.value.weight).subscribe(() => {
      this.bsModalRef.hide();
    });
  }
}

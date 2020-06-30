import { Component, OnInit } from '@angular/core';
import { Validators } from '@angular/forms';

import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';

import { OsdService } from '../../../../shared/api/osd.service';
import { CdFormBuilder } from '../../../../shared/forms/cd-form-builder';
import { CdFormGroup } from '../../../../shared/forms/cd-form-group';

@Component({
  selector: 'cd-osd-reweight-modal',
  templateUrl: './osd-reweight-modal.component.html',
  styleUrls: ['./osd-reweight-modal.component.scss']
})
export class OsdReweightModalComponent implements OnInit {
  currentWeight = 1;
  osdId: number;
  reweightForm: CdFormGroup;

  constructor(
    public activeModal: NgbActiveModal,
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

  reweight() {
    this.osdService
      .reweight(this.osdId, this.reweightForm.value.weight)
      .subscribe(() => this.activeModal.close());
  }
}

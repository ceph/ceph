import { Component, EventEmitter, Input, OnInit, Output } from '@angular/core';

import { BsModalRef } from 'ngx-bootstrap/modal';

import { OrchestratorService } from '../../../../shared/api/orchestrator.service';
import { ActionLabelsI18n } from '../../../../shared/constants/app.constants';
import { CdFormBuilder } from '../../../../shared/forms/cd-form-builder';
import { CdFormGroup } from '../../../../shared/forms/cd-form-group';
import { DriveGroup } from '../osd-form/drive-group.model';

@Component({
  selector: 'cd-osd-creation-preview-modal',
  templateUrl: './osd-creation-preview-modal.component.html',
  styleUrls: ['./osd-creation-preview-modal.component.scss']
})
export class OsdCreationPreviewModalComponent implements OnInit {
  @Input()
  driveGroup: DriveGroup;

  @Output()
  submitAction = new EventEmitter();

  action: string;
  formGroup: CdFormGroup;

  constructor(
    public bsModalRef: BsModalRef,
    public actionLabels: ActionLabelsI18n,
    private formBuilder: CdFormBuilder,
    private orchService: OrchestratorService
  ) {
    this.action = actionLabels.ADD;
    this.createForm();
  }

  ngOnInit() {}

  createForm() {
    this.formGroup = this.formBuilder.group({});
  }

  onSubmit() {
    this.orchService.osdCreate(this.driveGroup.spec).subscribe(
      undefined,
      () => {
        this.formGroup.setErrors({ cdSubmitButton: true });
      },
      () => {
        this.submitAction.emit();
        this.bsModalRef.hide();
      }
    );
  }
}

import { Component, EventEmitter, Input, Output } from '@angular/core';

import * as _ from 'lodash';
import { BsModalRef } from 'ngx-bootstrap/modal';

import { OsdService } from '../../../../shared/api/osd.service';
import { ActionLabelsI18n, URLVerbs } from '../../../../shared/constants/app.constants';
import { CdFormBuilder } from '../../../../shared/forms/cd-form-builder';
import { CdFormGroup } from '../../../../shared/forms/cd-form-group';
import { FinishedTask } from '../../../../shared/models/finished-task';
import { TaskWrapperService } from '../../../../shared/services/task-wrapper.service';

@Component({
  selector: 'cd-osd-creation-preview-modal',
  templateUrl: './osd-creation-preview-modal.component.html',
  styleUrls: ['./osd-creation-preview-modal.component.scss']
})
export class OsdCreationPreviewModalComponent {
  @Input()
  driveGroups: Object[] = [];

  @Output()
  submitAction = new EventEmitter();

  action: string;
  formGroup: CdFormGroup;

  constructor(
    public bsModalRef: BsModalRef,
    public actionLabels: ActionLabelsI18n,
    private formBuilder: CdFormBuilder,
    private osdService: OsdService,
    private taskWrapper: TaskWrapperService
  ) {
    this.action = actionLabels.CREATE;
    this.createForm();
  }

  createForm() {
    this.formGroup = this.formBuilder.group({});
  }

  onSubmit() {
    this.taskWrapper
      .wrapTaskAroundCall({
        task: new FinishedTask('osd/' + URLVerbs.CREATE, {
          tracking_id: _.join(_.map(this.driveGroups, 'service_id'), ', ')
        }),
        call: this.osdService.create(this.driveGroups)
      })
      .subscribe(
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

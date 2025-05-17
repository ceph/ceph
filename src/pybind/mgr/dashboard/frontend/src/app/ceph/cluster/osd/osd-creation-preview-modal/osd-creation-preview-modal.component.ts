import { Component, EventEmitter, Inject, Optional, Output } from '@angular/core';

import { BaseModal } from 'carbon-components-angular';
import _ from 'lodash';

import { OsdService } from '~/app/shared/api/osd.service';
import { ActionLabelsI18n, URLVerbs } from '~/app/shared/constants/app.constants';
import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';

@Component({
  selector: 'cd-osd-creation-preview-modal',
  templateUrl: './osd-creation-preview-modal.component.html',
  styleUrls: ['./osd-creation-preview-modal.component.scss']
})
export class OsdCreationPreviewModalComponent extends BaseModal{

  @Output()
  submitAction = new EventEmitter();

  action: string;
  formGroup: CdFormGroup;

  constructor(
    public actionLabels: ActionLabelsI18n,
    private formBuilder: CdFormBuilder,
    private osdService: OsdService,
    private taskWrapper: TaskWrapperService,

    @Optional() @Inject('driveGroups') public driveGroups: Object[],
  ) {
    super();
    this.action = actionLabels.CREATE;
    this.createForm();
  }

  createForm() {
    this.formGroup = this.formBuilder.group({});
  }

  onSubmit() {
    const trackingId = _.join(_.map(this.driveGroups, 'service_id'), ', ');
    this.taskWrapper
      .wrapTaskAroundCall({
        task: new FinishedTask('osd/' + URLVerbs.CREATE, {
          tracking_id: trackingId
        }),
        call: this.osdService.create(this.driveGroups, trackingId)
      })
      .subscribe({
        error: () => {
          this.formGroup.setErrors({ cdSubmitButton: true });
        },
        complete: () => {
          this.submitAction.emit();
          this.closeModal();
        }
      });
  }
}

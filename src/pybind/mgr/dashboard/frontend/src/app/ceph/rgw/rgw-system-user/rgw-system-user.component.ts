import { Component, EventEmitter, Output } from '@angular/core';
import { UntypedFormControl, Validators } from '@angular/forms';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { BaseModal } from 'carbon-components-angular';
import { RgwZoneService } from '~/app/shared/api/rgw-zone.service';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { NotificationService } from '~/app/shared/services/notification.service';

@Component({
  selector: 'cd-rgw-system-user',
  templateUrl: './rgw-system-user.component.html',
  styleUrls: ['./rgw-system-user.component.scss']
})
export class RgwSystemUserComponent extends BaseModal {
  multisiteSystemUserForm: CdFormGroup;
  zoneName: string;

  @Output()
  submitAction = new EventEmitter();

  constructor(
    public activeModal: NgbActiveModal,
    public actionLabels: ActionLabelsI18n,
    public rgwZoneService: RgwZoneService,
    public notificationService: NotificationService
  ) {
    super();
    this.createForm();
  }

  createForm() {
    this.multisiteSystemUserForm = new CdFormGroup({
      userName: new UntypedFormControl(null, {
        validators: [Validators.required]
      })
    });
  }

  submit() {
    const userName = this.multisiteSystemUserForm.getValue('userName');
    this.rgwZoneService.createSystemUser(userName, this.zoneName).subscribe(() => {
      this.submitAction.emit();
      this.notificationService.show(
        NotificationType.success,
        $localize`User: '${this.multisiteSystemUserForm.getValue('userName')}' created successfully`
      );
      this.closeModal();
    });
  }
}

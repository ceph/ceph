import { Component } from '@angular/core';
import { FormControl, Validators } from '@angular/forms';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { MulticlusterService } from '~/app/shared/api/multicluster.service';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { NotificationService } from '~/app/shared/services/notification.service';

@Component({
  selector: 'cd-multicluster-form',
  templateUrl: './multicluster-form.component.html',
  styleUrls: ['./multicluster-form.component.scss']
})
export class MulticlusterFormComponent {

  remoteClusterForm: CdFormGroup;

  constructor(
    public activeModal: NgbActiveModal,
    public multiclusterService: MulticlusterService,
    public actionLabels: ActionLabelsI18n,
    public notificationService: NotificationService
  ) {
    this.createForm();
  }
  ngOnInit(): void {
  }

  createForm() {
    this.remoteClusterForm = new CdFormGroup({
      apiToken: new FormControl('', {
        validators: [Validators.required]
      }),
      remoteClusterUrl: new FormControl(null, {
        validators: [
          Validators.required,
        ]
      })
    });
  }

  onSubmit() {
    const values = this.remoteClusterForm.value;
    this.multiclusterService.addRemoteCluster(values['apiToken'], values['remoteClusterUrl']).subscribe(
      (data) => {
        console.log(data);
        
        this.notificationService.show(
          NotificationType.success,
          $localize`Remote cluster added successfully`,
        );

        this.activeModal.close();
        window.location.reload();

      },
      () => {
        this.remoteClusterForm.setErrors({ cdSubmitButton: true });
      }
    );
  }

}

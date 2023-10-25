import { Component } from '@angular/core';
import { FormControl, Validators } from '@angular/forms';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { MultiClusterService } from '~/app/shared/api/multi-cluster.service';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { NotificationService } from '~/app/shared/services/notification.service';

@Component({
  selector: 'cd-multi-cluster-form',
  templateUrl: './multi-cluster-form.component.html',
  styleUrls: ['./multi-cluster-form.component.scss']
})
export class MultiClusterFormComponent {
  remoteClusterForm: CdFormGroup;
  showToken = false;

  constructor(
    public activeModal: NgbActiveModal,
    // public multiclusterService: MulticlusterService,
    public actionLabels: ActionLabelsI18n,
    public notificationService: NotificationService,
    private multiClusterService: MultiClusterService
  ) {
    this.createForm();
  }
  ngOnInit(): void {}

  createForm() {
    this.remoteClusterForm = new CdFormGroup({
      username: new FormControl('', {
        // validators: [Validators.required]
      }),
      password: new FormControl('', {
        // validators: [Validators.required]
      }),
      remoteClusterUrl: new FormControl(null, {
        validators: [Validators.required]
      }),
      apiToken: new FormControl(null, {
        // validators: [Validators.required]
      }),
      showToken: new FormControl(false),
      name: new FormControl(
        { value: null, disabled: true },
        {
          validators: []
        }
      )
    });
  }

  onSubmit() {
    const url = this.remoteClusterForm.getValue('remoteClusterUrl');
    const name = this.remoteClusterForm.getValue('name');
    const username = this.remoteClusterForm.getValue('username');
    const password = this.remoteClusterForm.getValue('password');
    const token = this.remoteClusterForm.getValue('apiToken');

    this.multiClusterService.addCluster(url, name, username, password, token).subscribe({
      error: () => this.remoteClusterForm.setErrors({ cdSubmitButton: true }),
      complete: () => {
        this.notificationService.show(
          NotificationType.success,
          $localize`Cluster added successfully`
        );
        this.activeModal.close();
        window.location.reload();
      }
    });
  }
}

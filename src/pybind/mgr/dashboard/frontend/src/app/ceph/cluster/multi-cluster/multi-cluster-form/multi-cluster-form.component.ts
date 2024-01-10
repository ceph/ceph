import { Component, EventEmitter, Output } from '@angular/core';
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

  @Output()
  submitAction = new EventEmitter();

  remoteClusterForm: CdFormGroup;
  showToken = false;
  interval: NodeJS.Timer;

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
      ),
      helperText: new FormControl('', {
        validators: [Validators.required]
      })
    });
  }

  ngOnDestroy() {
    clearInterval(this.interval);
  }


  onSubmit() {
    const url = this.remoteClusterForm.getValue('remoteClusterUrl');
    const name = this.remoteClusterForm.getValue('name');
    const helperText = this.remoteClusterForm.getValue('helperText');
    const username = this.remoteClusterForm.getValue('username');
    const password = this.remoteClusterForm.getValue('password');
    const token = this.remoteClusterForm.getValue('apiToken');

    this.multiClusterService
      .addCluster(url, name, helperText, username, password, token, window.location.origin)
      .subscribe({
        error: () => {
          this.remoteClusterForm.setErrors({ cdSubmitButton: true });
        },
        complete: () => {
          this.notificationService.show(
            NotificationType.success,
            $localize`Cluster added successfully`
          );   
          this.activeModal.close();
          this.submitAction.emit();
        }
      });
  }
}

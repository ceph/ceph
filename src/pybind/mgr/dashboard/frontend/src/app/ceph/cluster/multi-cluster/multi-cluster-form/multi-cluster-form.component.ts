import { Component, OnDestroy, OnInit } from '@angular/core';
import { FormControl, Validators } from '@angular/forms';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import _ from 'lodash';
import { Subscription } from 'rxjs';
import { MultiClusterService } from '~/app/shared/api/multi-cluster.service';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { NotificationService } from '~/app/shared/services/notification.service';

@Component({
  selector: 'cd-multi-cluster-form',
  templateUrl: './multi-cluster-form.component.html',
  styleUrls: ['./multi-cluster-form.component.scss']
})
export class MultiClusterFormComponent implements OnInit, OnDestroy {
  readonly endpoints = /^((https?:\/\/)|(www.))(?:([a-zA-Z]+)|(\d+\.\d+.\d+.\d+)):\d{2,5}\/?$/;
  readonly ipv4Rgx = /^((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$/i;
  readonly ipv6Rgx = /^(?:[a-f0-9]{1,4}:){7}[a-f0-9]{1,4}$/i;
  remoteClusterForm: CdFormGroup;
  showToken = false;
  connectionVerified: boolean;
  connectionMessage = '';
  private subs = new Subscription();
  showCrossOriginError = false;
  crossOriginCmd: string;

  constructor(
    public activeModal: NgbActiveModal,
    public actionLabels: ActionLabelsI18n,
    public notificationService: NotificationService,
    private multiClusterService: MultiClusterService
  ) {
    this.createForm();
  }
  ngOnInit(): void {}

  createForm() {
    this.remoteClusterForm = new CdFormGroup({
      showToken: new FormControl(false),
      username: new FormControl('', [
        CdValidators.requiredIf({
          showToken: false
        })
      ]),
      password: new FormControl('', [
        CdValidators.requiredIf({
          showToken: false
        })
      ]),
      remoteClusterUrl: new FormControl(null, {
        validators: [
          CdValidators.custom('endpoint', (value: string) => {
            if (_.isEmpty(value)) {
              return false;
            } else {
              return (
                !this.endpoints.test(value) &&
                !this.ipv4Rgx.test(value) &&
                !this.ipv6Rgx.test(value)
              );
            }
          }),
          Validators.required
        ]
      }),
      apiToken: new FormControl('', [
        CdValidators.requiredIf({
          showToken: true
        })
      ]),
      clusterAlias: new FormControl('', {
        validators: [Validators.required]
      })
    });
  }

  ngOnDestroy() {
    this.subs.unsubscribe();
  }

  onSubmit() {
    const url = this.remoteClusterForm.getValue('remoteClusterUrl');
    const clusterAlias = this.remoteClusterForm.getValue('clusterAlias');
    const username = this.remoteClusterForm.getValue('username');
    const password = this.remoteClusterForm.getValue('password');
    const token = this.remoteClusterForm.getValue('apiToken');

    this.subs.add(
      this.multiClusterService
        .addCluster(url, clusterAlias, username, password, token, window.location.origin)
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
          }
        })
    );
  }

  verifyConnection() {
    const url = this.remoteClusterForm.getValue('remoteClusterUrl');
    const username = this.remoteClusterForm.getValue('username');
    const password = this.remoteClusterForm.getValue('password');
    const token = this.remoteClusterForm.getValue('apiToken');

    this.subs.add(
      this.multiClusterService
        .verifyConnection(url, username, password, token)
        .subscribe((resp: string) => {
          switch (resp) {
            case 'Connection successful':
              this.connectionVerified = true;
              this.connectionMessage = 'Connection Verified Successfully';
              this.notificationService.show(
                NotificationType.success,
                $localize`Connection Verified Successfully`
              );
              break;

            case 'Connection refused':
              this.connectionVerified = false;
              this.showCrossOriginError = true;
              this.connectionMessage = resp;
              this.crossOriginCmd = `ceph config set mgr mgr/dashboard/cross_origin_url ${window.location.origin} `;
              this.notificationService.show(
                NotificationType.error,
                $localize`Connection to the cluster failed`
              );
              break;

            default:
              this.connectionVerified = false;
              this.connectionMessage = resp;
              this.notificationService.show(
                NotificationType.error,
                $localize`Connection to the cluster failed`
              );
              break;
          }
        })
    );
  }
}

import { Component, EventEmitter, OnDestroy, OnInit, Output } from '@angular/core';
import { AbstractControl, FormControl, Validators } from '@angular/forms';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import _ from 'lodash';
import { Subscription } from 'rxjs';
import { MultiClusterService } from '~/app/shared/api/multi-cluster.service';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { MultiCluster } from '~/app/shared/models/multi-cluster';
import { NotificationService } from '~/app/shared/services/notification.service';

@Component({
  selector: 'cd-multi-cluster-form',
  templateUrl: './multi-cluster-form.component.html',
  styleUrls: ['./multi-cluster-form.component.scss']
})
export class MultiClusterFormComponent implements OnInit, OnDestroy {
  @Output()
  submitAction = new EventEmitter();
  readonly endpoints = /^((https?:\/\/)|(www.))(?:([a-zA-Z]+)|(\d+\.\d+.\d+.\d+)):\d{2,5}\/?$/;
  readonly ipv4Rgx = /^((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$/i;
  readonly ipv6Rgx = /^(?:[a-f0-9]{1,4}:){7}[a-f0-9]{1,4}$/i;
  clusterApiUrlCmd = 'ceph mgr services';
  remoteClusterForm: CdFormGroup;
  connectionVerified: boolean;
  connectionMessage = '';
  private subs = new Subscription();
  action: string;
  cluster: MultiCluster;
  clustersData: MultiCluster[];
  clusterAliasNames: string[];
  clusterUrls: string[];
  clusterUsers: string[];
  clusterUrlUserMap: Map<string, string>;
  hubUrl: string;

  constructor(
    public activeModal: NgbActiveModal,
    public actionLabels: ActionLabelsI18n,
    public notificationService: NotificationService,
    private multiClusterService: MultiClusterService
  ) {
    this.subs.add(
      this.multiClusterService.subscribe((resp: any) => {
        this.hubUrl = resp['hub_url'];
      })
    );
    this.createForm();
  }
  ngOnInit(): void {
    if (this.action === 'edit') {
      this.remoteClusterForm.get('remoteClusterUrl').setValue(this.cluster.url);
      this.remoteClusterForm.get('remoteClusterUrl').disable();
      this.remoteClusterForm.get('clusterAlias').setValue(this.cluster.cluster_alias);
      this.remoteClusterForm.get('ssl').setValue(this.cluster.ssl_verify);
      this.remoteClusterForm.get('ssl_cert').setValue(this.cluster.ssl_certificate);
    }
    if (this.action === 'reconnect') {
      this.remoteClusterForm.get('remoteClusterUrl').setValue(this.cluster.url);
      this.remoteClusterForm.get('remoteClusterUrl').disable();
      this.remoteClusterForm.get('clusterAlias').setValue(this.cluster.cluster_alias);
      this.remoteClusterForm.get('clusterAlias').disable();
      this.remoteClusterForm.get('username').setValue(this.cluster.user);
      this.remoteClusterForm.get('username').disable();
      this.remoteClusterForm.get('ssl').setValue(this.cluster.ssl_verify);
      this.remoteClusterForm.get('ssl_cert').setValue(this.cluster.ssl_certificate);
    }
    [this.clusterAliasNames, this.clusterUrls, this.clusterUsers] = [
      'cluster_alias',
      'url',
      'user'
    ].map((prop) => this.clustersData?.map((cluster) => cluster[prop]));
  }

  createForm() {
    this.remoteClusterForm = new CdFormGroup({
      // showToken: new FormControl(false),
      username: new FormControl('', [
        CdValidators.custom('uniqueUrlandUser', (username: string) => {
          let remoteClusterUrl = '';
          if (
            this.remoteClusterForm &&
            this.remoteClusterForm.getValue('remoteClusterUrl') &&
            this.remoteClusterForm.getValue('remoteClusterUrl').endsWith('/')
          ) {
            remoteClusterUrl = this.remoteClusterForm.getValue('remoteClusterUrl').slice(0, -1);
          } else if (this.remoteClusterForm) {
            remoteClusterUrl = this.remoteClusterForm.getValue('remoteClusterUrl');
          }
          return (
            this.remoteClusterForm &&
            this.clusterUrls?.includes(remoteClusterUrl) &&
            this.clusterUsers?.includes(username)
          );
        })
      ]),
      password: new FormControl('', []),
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
          CdValidators.custom('hubUrlCheck', (remoteClusterUrl: string) => {
            return this.action === 'connect' && remoteClusterUrl?.includes(this.hubUrl);
          }),
          Validators.required
        ]
      }),
      // apiToken: new FormControl('', [
      //   CdValidators.requiredIf({
      //     showToken: true
      //   })
      // ]),
      clusterAlias: new FormControl(null, {
        validators: [
          Validators.required,
          CdValidators.custom('uniqueName', (clusterAlias: string) => {
            return (
              (this.action === 'connect' || this.action === 'edit') &&
              this.clusterAliasNames &&
              this.clusterAliasNames.indexOf(clusterAlias) !== -1
            );
          })
        ]
      }),
      ssl: new FormControl(false),
      ttl: new FormControl(15),
      ssl_cert: new FormControl('', {
        validators: [
          CdValidators.requiredIf({
            ssl: true
          })
        ]
      })
    });
  }

  ngOnDestroy() {
    this.subs.unsubscribe();
  }

  handleError(error: any): void {
    if (error.error.code === 'connection_refused') {
      this.connectionVerified = false;
      this.connectionMessage = error.error.detail;
    } else {
      this.connectionVerified = false;
      this.connectionMessage = error.error.detail;
    }
    this.remoteClusterForm.setErrors({ cdSubmitButton: true });
    this.notificationService.show(
      NotificationType.error,
      $localize`Connection to the cluster failed`
    );
  }

  handleSuccess(message?: string): void {
    this.notificationService.show(NotificationType.success, message);
    this.submitAction.emit();
    this.activeModal.close();
  }

  convertToHours(value: number): number {
    return value * 24; // Convert days to hours
  }

  onSubmit() {
    const url = this.remoteClusterForm.getValue('remoteClusterUrl');
    const updatedUrl = url.endsWith('/') ? url.slice(0, -1) : url;
    const clusterAlias = this.remoteClusterForm.getValue('clusterAlias');
    const username = this.remoteClusterForm.getValue('username');
    const password = this.remoteClusterForm.getValue('password');
    const ssl = this.remoteClusterForm.getValue('ssl');
    const ttl = this.convertToHours(this.remoteClusterForm.getValue('ttl'));
    const ssl_certificate = this.remoteClusterForm.getValue('ssl_cert')?.trim();

    const commonSubscribtion = {
      error: (error: any) => this.handleError(error),
      next: (response: any) => {
        if (response === true) {
          this.handleSuccess($localize`Cluster connected successfully`);
        }
      }
    };

    switch (this.action) {
      case 'edit':
        this.subs.add(
          this.multiClusterService
            .editCluster(this.cluster.url, clusterAlias, this.cluster.user)
            .subscribe({
              ...commonSubscribtion,
              complete: () => this.handleSuccess($localize`Cluster updated successfully`)
            })
        );
        break;
      case 'reconnect':
        this.subs.add(
          this.multiClusterService
            .reConnectCluster(updatedUrl, username, password, ssl, ssl_certificate, ttl)
            .subscribe(commonSubscribtion)
        );
        break;
      case 'connect':
        this.subs.add(
          this.multiClusterService
            .addCluster(
              updatedUrl,
              clusterAlias,
              username,
              password,
              window.location.origin,
              ssl,
              ssl_certificate,
              ttl
            )
            .subscribe(commonSubscribtion)
        );
        break;
      default:
        break;
    }
  }

  fileUpload(files: FileList, controlName: string) {
    const file: File = files[0];
    const reader = new FileReader();
    reader.addEventListener('load', (event: ProgressEvent<FileReader>) => {
      const control: AbstractControl = this.remoteClusterForm.get(controlName);
      control.setValue(event.target.result);
      control.markAsDirty();
      control.markAsTouched();
      control.updateValueAndValidity();
    });
    reader.readAsText(file, 'utf8');
  }
}

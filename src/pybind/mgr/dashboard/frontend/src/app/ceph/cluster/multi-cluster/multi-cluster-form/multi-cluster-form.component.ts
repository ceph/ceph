import { Component, EventEmitter, OnDestroy, OnInit, Output } from '@angular/core';
import { AbstractControl, FormControl, Validators } from '@angular/forms';
import { Router } from '@angular/router';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import _ from 'lodash';
import { Subscription } from 'rxjs';
import { MultiClusterService } from '~/app/shared/api/multi-cluster.service';
import { ActionLabelsI18n, USER } from '~/app/shared/constants/app.constants';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { MultiCluster } from '~/app/shared/models/multi-cluster';
import { NotificationService } from '~/app/shared/services/notification.service';

@Component({
  selector: 'cd-multi-cluster-form',
  templateUrl: './multi-cluster-form.component.html',
  styleUrls: ['./multi-cluster-form.component.scss'],
  standalone: false
})
export class MultiClusterFormComponent implements OnInit, OnDestroy {
  @Output()
  submitAction = new EventEmitter();
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
  loading = false;
  formPatched = false;
  clusterTokenStatus: { [key: string]: any } = {};

  constructor(
    public activeModal: NgbActiveModal,
    public actionLabels: ActionLabelsI18n,
    public notificationService: NotificationService,
    private multiClusterService: MultiClusterService,
    private router: Router
  ) {
    this.subs.add(
      this.multiClusterService.subscribe((resp: any) => {
        this.hubUrl = resp['hub_url'];
      })
    );
    this.prepareFormForAction();
  }

  ngOnInit(): void {
    this.createForm();
    const clusterName = this.router.url.split('/').pop();
    this.subs.add(
      this.multiClusterService.subscribe((resp: any) => {
        if (resp && resp['config']) {
          this.clustersData = Object.values(resp['config']).flat() as MultiCluster[];
          this.cluster = this.clustersData.find((c) => c.name === clusterName);
          [this.clusterAliasNames, this.clusterUrls, this.clusterUsers] = [
            'cluster_alias',
            'url',
            USER
          ].map((prop) => this.clustersData.map((c) => (c as any)[prop]));
          if (!this.formPatched && this.cluster) {
            this.formPatched = true;
            this.prepareEditForm(this.cluster);
            this.prepareReconnectForm(this.cluster);
          }
        }
      })
    );

    this.subs.add(
      this.multiClusterService.subscribeClusterTokenStatus((resp: object) => {
        this.clusterTokenStatus = resp;
        this.checkClusterConnectionStatus();
      })
    );
  }

  checkClusterConnectionStatus() {
    if (this.clusterTokenStatus && this.clustersData) {
      this.clustersData.forEach((cluster: MultiCluster) => {
        const clusterStatus = this.clusterTokenStatus[cluster.name];
        if (clusterStatus !== undefined) {
          cluster.cluster_connection_status = clusterStatus.status;
          cluster.ttl = clusterStatus.time_left;
        } else {
          cluster.cluster_connection_status = 2;
        }
        if (cluster.cluster_alias === 'local-cluster') {
          cluster.cluster_connection_status = 0;
        }
      });
    }
  }

  prepareEditForm(cluster: MultiCluster) {
    if (this.action === 'Edit') {
      this.remoteClusterForm.get('remoteClusterUrl').setValue(cluster.url);
      this.remoteClusterForm.get('remoteClusterUrl').disable();
      this.remoteClusterForm.get('clusterAlias').setValue(cluster.cluster_alias);
      this.remoteClusterForm.get('username').setValue(cluster.user);
      this.remoteClusterForm.get('username').disable();
      this.remoteClusterForm.get('ssl').setValue(cluster.ssl_verify);
      this.remoteClusterForm.get('ssl_cert').setValue(cluster.ssl_certificate);
    }
  }

  prepareReconnectForm(cluster: MultiCluster) {
    if (this.action === 'Reconnect') {
      this.remoteClusterForm.get('remoteClusterUrl').setValue(cluster.url);
      this.remoteClusterForm.get('remoteClusterUrl').disable();
      this.remoteClusterForm.get('clusterAlias').setValue(cluster.cluster_alias);
      this.remoteClusterForm.get('clusterAlias').disable();
      this.remoteClusterForm.get('username').setValue(cluster.user);
      this.remoteClusterForm.get('username').disable();
      this.remoteClusterForm.get('ssl').setValue(cluster.ssl_verify);
      this.remoteClusterForm.get('ssl_cert').setValue(cluster.ssl_certificate);
    }
  }

  createForm() {
    this.remoteClusterForm = new CdFormGroup({
      username: new FormControl('', [
        Validators.required,
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
            this.action !== 'Edit' &&
            this.remoteClusterForm &&
            this.clusterUrls?.includes(remoteClusterUrl) &&
            this.clusterUsers?.includes(username)
          );
        })
      ]),
      password: new FormControl(null, {
        validators: [
          Validators.required,
          CdValidators.custom('requiredNotEdit', (value: string) => {
            return this.action !== 'Edit' && !value;
          })
        ]
      }),
      remoteClusterUrl: new FormControl(null, {
        validators: [
          CdValidators.url,
          CdValidators.custom('hubUrlCheck', (remoteClusterUrl: string) => {
            if (this.action === 'Connect' && remoteClusterUrl?.includes(this.hubUrl)) {
              return true;
            }
            return false;
          }),
          Validators.required
        ]
      }),
      clusterAlias: new FormControl(null, {
        validators: [
          Validators.required,
          CdValidators.custom('uniqueName', (clusterAlias: string) => {
            return (
              (this.action === 'Connect' || this.action === 'Edit') &&
              this.clusterAliasNames &&
              this.clusterAliasNames.indexOf(clusterAlias) !== -1 &&
              this.cluster?.cluster_alias &&
              this.cluster.cluster_alias !== clusterAlias
            );
          })
        ]
      }),
      ttl: new FormControl(15),
      ssl: new FormControl(false),
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

  prepareFormForAction() {
    const url = this.router.url;
    if (url.startsWith('/multi-cluster/manage-clusters/connect')) {
      this.action = this.actionLabels.CONNECT;
    } else if (url.startsWith('/multi-cluster/manage-clusters/reconnect')) {
      this.action = this.actionLabels.RECONNECT;
    } else if (url.startsWith('/multi-cluster/manage-clusters/edit')) {
      this.action = this.actionLabels.EDIT;
    }
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
    const currentRoute = '/multi-cluster/manage-clusters';
    this.multiClusterService.refreshMultiCluster(currentRoute);
    this.checkClusterConnectionStatus();
    this.multiClusterService.isClusterAdded(true);
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
      case 'Edit':
        this.subs.add(
          this.multiClusterService
            .editCluster(
              this.cluster.name,
              url,
              clusterAlias,
              this.cluster.user,
              ssl,
              ssl_certificate
            )
            .subscribe({
              ...commonSubscribtion,
              complete: () => this.handleSuccess($localize`Cluster updated successfully`)
            })
        );
        break;
      case 'Reconnect':
        this.subs.add(
          this.multiClusterService
            .reConnectCluster(updatedUrl, username, password, ssl, ssl_certificate, ttl)
            .subscribe(commonSubscribtion)
        );
        break;
      case 'Connect':
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

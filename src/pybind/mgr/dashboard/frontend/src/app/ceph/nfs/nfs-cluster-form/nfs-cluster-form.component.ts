import { Component, OnInit, ViewChild } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import _ from 'lodash';
import { UntypedFormControl } from '@angular/forms';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { NfsService } from '~/app/shared/api/nfs.service';
import { getFsalFromRoute, getPathfromFsal } from '../utils';
import { SUPPORTED_FSAL } from '../models/nfs.fsal';
import { NfsRateLimitComponent } from '../nfs-rate-limit/nfs-rate-limit.component';

import { concat as observableConcat, Observable } from 'rxjs';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { NotificationService } from '~/app/shared/services/notification.service';
import { NFSBwIopConfig } from '../models/nfs-cluster-config';
import { CdForm } from '~/app/shared/forms/cd-form';

@Component({
  selector: 'cd-nfs-cluster-form',
  templateUrl: './nfs-cluster-form.component.html',
  styleUrls: ['./nfs-cluster-form.component.scss']
})
export class NfsClusterFormComponent extends CdForm implements OnInit {
  @ViewChild(NfsRateLimitComponent, { static: false })
  nfsRateLimitComponent!: NfsRateLimitComponent;

  action: string;
  nfsForm: CdFormGroup;
  resource: string;
  isEdit = false;
  fsal: SUPPORTED_FSAL;
  id: string;
  qosValue: string;
  nfsBwIopConfig: NFSBwIopConfig;
  submitObservables: Observable<Object>[] = [];

  constructor(
    public actionLabels: ActionLabelsI18n,
    private route: ActivatedRoute,
    private router: Router,
    private nfsService: NfsService,
    private notificationService: NotificationService
  ) {
    super();
    this.resource = $localize`Cluster`;
  }

  ngOnInit(): void {
    this.createClusterForm();
    this.id = this.route.snapshot.paramMap.get('cluster_id');
    this.fsal = getFsalFromRoute(this.router.url);
    if (this.router.url.startsWith(`/${getPathfromFsal(this.fsal)}/nfs/edit`)) {
      this.isEdit = true;
    }
    if (this.isEdit) {
      this.action = this.actionLabels.EDIT;
      this.nfsService.getClusterBandwidthOpsConfig(this.id).subscribe((data: NFSBwIopConfig) => {
        this.nfsBwIopConfig = data;
        this.nfsForm.get('cluster_id').setValue(this.id);
      });
      this.loadingReady();
    } else {
      this.action = this.actionLabels.CREATE;
    }
  }
  childCompErrorHandler(event: Event) {
    this.nfsForm.addControl('rateLimit', event);
  }
  createClusterForm() {
    this.nfsForm = new CdFormGroup({
      cluster_id: new UntypedFormControl({ value: '', disabled: true })
    });
  }

  goToListView() {
    this.router.navigate([`/${getPathfromFsal(this.fsal)}/nfs`]);
  }

  submitAction() {
    let notificationTitle: string;
    if (this.nfsForm.pristine && this.nfsRateLimitComponent.rateLimitForm.pristine) {
      this.goToListView();
      return;
    }

    const clusterFormObj = this.nfsForm.getRawValue();
    delete clusterFormObj.rateLimit;

    let ratelimitBw: NFSBwIopConfig = this.nfsRateLimitComponent.getRateLimitFormValue();
    let ratelimitOpsValue: NFSBwIopConfig = this.nfsRateLimitComponent.getRateLimitOpsFormValue();
    ratelimitBw = {
      ...ratelimitBw,
      ...clusterFormObj,
      disable_qos: !ratelimitBw?.enable_qos
    };
    delete ratelimitBw.enable_qos;

    ratelimitOpsValue = {
      ...ratelimitOpsValue,
      ...clusterFormObj,
      disable_Ops: !ratelimitOpsValue?.enable_ops
    };
    delete ratelimitOpsValue.enable_ops;

    this.submitObservables.push(
      this.nfsService.enableQosBandwidthForCLuster(ratelimitBw),
      this.nfsService.enableQosOpsForCLuster(ratelimitOpsValue)
    );

    notificationTitle = $localize`Cluster QoS Type Configured for '${this.id}'`;

    observableConcat(...this.submitObservables).subscribe({
      next: () => {
        this.notificationService.show(NotificationType.success, notificationTitle);
      },
      error: () => {
        this.nfsForm.setErrors({ cdSubmitButton: true });
      },
      complete: () => {
        this.goToListView();
      }
    });
  }
}

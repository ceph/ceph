import { Component, OnInit } from '@angular/core';
import { Router } from '@angular/router';
import { Validators } from '@angular/forms';
import { forkJoin, Observable } from 'rxjs';
import { map } from 'rxjs/operators';

import { OrchestratorService } from '~/app/shared/api/orchestrator.service';
import { NfsService } from '~/app/shared/api/nfs.service';
import { HostService } from '~/app/shared/api/host.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { CdForm } from '~/app/shared/forms/cd-form';
import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';

@Component({
  selector: 'cd-nfs-cluster-form',
  templateUrl: './nfs-cluster-form.component.html',
  standalone: false,
  styleUrls: ['./nfs-cluster-form.component.scss']
})
export class NfsClusterFormComponent extends CdForm implements OnInit {
  nfsForm: CdFormGroup;
  hostsAndLabels$: Observable<{ hosts: any[]; labels: any[] }>;
  hasOrchestrator: boolean;

  selectedHosts: string[] = [];
  selectedLabels: string[] = [];

  action: string;
  resource: string;

  ingressModes = [
    { value: 'default', label: 'default' },
    { value: 'keepalive-only', label: 'keepalive-only' },
    { value: 'haproxy-standard', label: 'haproxy-standard' },
    { value: 'haproxy-protocol', label: 'haproxy-protocol' }
  ];

  constructor(
    private nfsService: NfsService,
    private taskWrapper: TaskWrapperService,
    private router: Router,
    private hostService: HostService,
    private orchService: OrchestratorService,
    private formBuilder: CdFormBuilder,
    public actionLabels: ActionLabelsI18n
  ) {
    super();
    this.resource = $localize`NFS Cluster`;
  }

  ngOnInit(): void {
    this.action = this.actionLabels.CREATE;

    this.hostsAndLabels$ = forkJoin({
      hosts: this.hostService.getAllHosts(),
      labels: this.hostService.getLabels()
    }).pipe(
      map(({ hosts, labels }) => ({
        hosts: hosts.map((host: any) => ({ content: host['hostname'] })),
        labels: labels.map((label: string) => ({ content: label }))
      }))
    );

    this.createForm();
    this.loadingReady();
  }

  createForm() {
    this.nfsForm = this.formBuilder.group({
      cluster_id: [
        '',
        {
          validators: [Validators.required]
        }
      ],
      placement: [],
      hosts: [[]],
      label: [
        null,
        [
          CdValidators.requiredIf({
            placement: 'label'
          })
        ]
      ],
      count: [1, [Validators.min(1)]],
      ingress: [false],
      virtual_ip: [
        '',
        [
          CdValidators.requiredIf({
            ingress: true
          })
        ]
      ],
      ingress_mode: ['default'],
      port: ['', [Validators.min(1)]]
    });

    this.orchService.status().subscribe((status) => {
      this.hasOrchestrator = status.available;
      this.nfsForm.get('placement').setValue(this.hasOrchestrator ? 'hosts' : '');
    });
  }

  multiSelector(event: any, field: 'label' | 'hosts') {
    if (field === 'hosts') {
      this.selectedHosts = event.map((host: any) => host.content);
    } else {
      this.selectedLabels = event.map((label: any) => label.content);
    }
  }

  submitAction() {
    this.nfsForm.markAllAsTouched();

    if (this.nfsForm.invalid) {
      return;
    }

    const values = this.nfsForm.getRawValue();

    // Validate virtual_ip when ingress is enabled
    if (values.ingress && !values.virtual_ip) {
      this.nfsForm.get('virtual_ip').setErrors({ required: true });
      return;
    }

    const payload: any = {
      cluster_id: values.cluster_id,
      ingress: !!values.ingress,
      port: values.port || undefined
    };

    const placementSpec = this.getPlacementSpec(values);
    if (placementSpec && Object.keys(placementSpec).length > 0) {
      payload.placement = placementSpec;
    }

    if (values.ingress) {
      if (values.virtual_ip) {
        payload.virtual_ip = values.virtual_ip;
      }
      if (values.ingress_mode) {
        payload.ingress_mode = values.ingress_mode;
      }
    }

    this.taskWrapper
      .wrapTaskAroundCall({
        task: new FinishedTask('nfs/cluster/create', { cluster_id: payload.cluster_id }),
        call: this.nfsService.createCluster(payload)
      })
      .subscribe({
        complete: () => {
          this.router.navigate(['/cephfs/nfs']);
        },
        error: () => {
          this.nfsForm.setErrors({ cdSubmitButton: true });
        }
      });
  }

  getPlacementSpec(values: any) {
    const placement: any = {};

    if (values.count && values.count > 1) {
      placement.count = values.count;
    }

    switch (values.placement) {
      case 'hosts':
        if (this.selectedHosts.length > 0) {
          placement.hosts = this.selectedHosts;
          if (values.count) {
            placement.count = values.count;
          }
        }
        break;
      case 'label':
        if (this.selectedLabels.length > 0) {
          placement.label = this.selectedLabels[0];
          if (values.count) {
            placement.count = values.count;
          }
        }
        break;
    }

    return placement;
  }
}

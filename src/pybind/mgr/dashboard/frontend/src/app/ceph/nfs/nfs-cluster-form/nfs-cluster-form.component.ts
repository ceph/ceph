import { Component, DestroyRef, inject, OnInit } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { FormArray, FormControl, Validators } from '@angular/forms';
import { Router } from '@angular/router';
import { forkJoin, Observable } from 'rxjs';
import { catchError, map } from 'rxjs/operators';

import { NfsService } from '~/app/shared/api/nfs.service';
import { HostService } from '~/app/shared/api/host.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { CdForm } from '~/app/shared/forms/cd-form';
import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import type { NFSBindAddr, NFSClusterPayload } from '../models/nfs-cluster-config';

@Component({
  selector: 'cd-nfs-cluster-form',
  templateUrl: './nfs-cluster-form.component.html',
  standalone: false,
  styleUrls: ['./nfs-cluster-form.component.scss']
})
export class NfsClusterFormComponent extends CdForm implements OnInit {
  private destroyRef = inject(DestroyRef);

  nfsForm: CdFormGroup;
  hostsAndLabels$: Observable<{ hosts: any[]; labels: any[] }>;
  clusterNames: string[] = [];
  showDeploymentSettings = false;
  showAdvancedNetwork = false;

  selectedHosts: string[] = [];
  selectedIngressHosts: string[] = [];

  action: string;

  readonly PLACEMENT = {
    auto: 'auto',
    host: 'hosts',
    custom: 'custom'
  };

  readonly CUSTOM_PLACEMENT = {
    label: 'label',
    hostPattern: 'host_pattern'
  };

  ingressModes = [
    {
      value: 'haproxy-protocol',
      label: $localize`Balance traffic across different IPs (HAProxy protocol)`
    },
    {
      value: 'haproxy-standard',
      label: $localize`Balance traffic only (HAProxy standard)`
    },
    {
      value: 'keepalive-only',
      label: $localize`Failover only (Keepalive only)`
    }
  ];

  ingressPlacementModes = [
    {
      value: 'nfs',
      label: $localize`Same hosts as the NFS service`
    },
    {
      value: 'custom',
      label: $localize`Different hosts than the NFS service`
    }
  ];

  constructor(
    private nfsService: NfsService,
    private taskWrapper: TaskWrapperService,
    private router: Router,
    private hostService: HostService,
    private formBuilder: CdFormBuilder,
    public actionLabels: ActionLabelsI18n
  ) {
    super();
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

    this.nfsService
      .listClusters(false)
      .pipe(
        catchError(() => []),
        takeUntilDestroyed(this.destroyRef)
      )
      .subscribe((names: any) => {
        this.clusterNames = names as string[];
        this.createForm();
        this.loadingReady();
      });
  }

  get networks() {
    return this.nfsForm.get('networks') as FormArray;
  }

  get bind_addrs() {
    return this.nfsForm.get('bind_addrs') as FormArray;
  }

  createForm() {
    this.nfsForm = this.formBuilder.group({
      cluster_id: [
        '',
        [
          Validators.required,
          Validators.pattern(/^[a-z0-9]+(?:-[a-z0-9]+)*$/),
          CdValidators.custom('uniqueName', (id: string) => this.clusterNames?.includes(id))
        ]
      ],
      placement: [this.PLACEMENT.auto],
      hosts: [[]],
      label: [
        null,
        [
          CdValidators.requiredIf({
            placement: this.PLACEMENT.custom,
            custom_placement_type: this.CUSTOM_PLACEMENT.label
          })
        ]
      ],
      custom_placement_type: [this.CUSTOM_PLACEMENT.label],
      host_pattern: [
        '',
        [
          CdValidators.requiredIf({
            placement: this.PLACEMENT.custom,
            custom_placement_type: this.CUSTOM_PLACEMENT.hostPattern
          })
        ]
      ],
      count: [1, [Validators.min(1)]],
      protocol_version: ['nfsv4'],
      port: [2049, [CdValidators.number(false), Validators.min(1), Validators.max(65535)]],
      networks: this.formBuilder.array([new FormControl('', [CdValidators.ipv4Cidr()])]),
      bind_addrs: this.formBuilder.array([
        this.formBuilder.group({
          hostname: [''],
          ip: ['', [CdValidators.ip()]]
        })
      ]),
      ingress: [false],
      ingress_placement_mode: ['nfs'],
      ingress_hosts: [[]],
      virtual_ip: [
        '',
        [
          CdValidators.requiredIf({
            ingress: true
          }),
          CdValidators.ipv4Cidr()
        ]
      ],
      ingress_mode: ['haproxy-protocol'],
      enable_rdma: [false],
      rdma_port: [20049, [CdValidators.number(false), Validators.min(1), Validators.max(65535)]],
      monitoring_port: [
        9587,
        [CdValidators.number(false), Validators.min(1), Validators.max(65535)]
      ]
    });
  }

  addNetwork() {
    this.networks.push(new FormControl('', [CdValidators.ipv4Cidr()]));
  }

  removeNetwork(index: number) {
    this.networks.removeAt(index);
  }

  addBindAddr() {
    this.bind_addrs.push(
      this.formBuilder.group({
        hostname: [''],
        ip: ['', [CdValidators.ip()]]
      })
    );
  }

  removeBindAddr(index: number) {
    this.bind_addrs.removeAt(index);
  }

  multiSelector(event: any, field: 'hosts' | 'ingress_hosts') {
    const values = event.map((item: any) => item.content);
    switch (field) {
      case this.PLACEMENT.host:
        this.selectedHosts = values;
        break;
      case 'ingress_hosts':
        this.selectedIngressHosts = values;
        break;
    }
  }

  submitAction() {
    this.nfsForm.markAllAsTouched();

    if (this.nfsForm.invalid) {
      return;
    }

    const values = this.nfsForm.getRawValue();
    const virtualIp = values.virtual_ip?.trim();

    const payload: NFSClusterPayload = {
      cluster_id: values.cluster_id
    };

    if (values.port) {
      payload.port = values.port;
    }

    const placementSpec = this.getPlacementSpec(values);
    if (placementSpec && Object.keys(placementSpec).length > 0) {
      payload.placement = placementSpec;
    }

    const networks = this.collectNetworks(values);
    if (networks.length > 0) {
      payload.networks = networks;
    }

    const bindAddrs = this.collectHostIpEntries(values.bind_addrs);
    if (bindAddrs.length > 0) {
      payload.bind_addrs = bindAddrs;
    }

    if (values.ingress) {
      payload.ingress = true;
      payload.virtual_ip = virtualIp;
      if (values.ingress_mode) {
        payload.ingress_mode = values.ingress_mode;
      }
      if (values.ingress_placement_mode === 'custom') {
        const ingressPlacementSpec = this.getIngressPlacementSpec();
        if (ingressPlacementSpec && Object.keys(ingressPlacementSpec).length > 0) {
          payload.ingress_placement = ingressPlacementSpec;
        }
      }
    }

    if (values.protocol_version === 'nfsv3') {
      payload.enable_nfsv3 = true;
    }

    if (values.enable_rdma) {
      payload.enable_rdma = true;
      if (values.rdma_port) {
        payload.rdma_port = values.rdma_port;
      }
    }

    if (values.monitoring_port) {
      payload.monitoring_port = values.monitoring_port;
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
    const placement: any = { count: values.count };

    switch (values.placement) {
      case this.PLACEMENT.host:
        if (this.selectedHosts.length > 0) {
          placement.hosts = this.selectedHosts;
        }
        break;
      case this.PLACEMENT.custom:
        if (values.custom_placement_type === this.CUSTOM_PLACEMENT.label && values.label) {
          placement.label = values.label?.content ?? values.label;
        } else if (
          values.custom_placement_type === this.CUSTOM_PLACEMENT.hostPattern &&
          values.host_pattern
        ) {
          placement.host_pattern = values.host_pattern;
        }
        break;
    }

    return placement;
  }

  getIngressPlacementSpec() {
    const placement: any = {};
    if (this.selectedIngressHosts.length > 0) {
      placement.hosts = this.selectedIngressHosts;
      placement.count = this.selectedIngressHosts.length;
    }
    return placement;
  }

  private collectNetworks(values: any): string[] {
    const networks = ((values.networks || []) as string[])
      .map((network) => network?.trim())
      .filter((network): network is string => !!network);
    return [...new Set(networks)];
  }

  private collectHostIpEntries(entries: any[]): NFSBindAddr[] {
    return (entries || [])
      .map((entry: any) => {
        const hostname =
          typeof entry?.hostname === 'string'
            ? entry.hostname.trim()
            : (entry?.hostname?.content?.trim() ?? '');
        const ip = typeof entry?.ip === 'string' ? entry.ip.trim() : '';
        return { hostname, ip };
      })
      .filter((entry: NFSBindAddr) => entry.hostname && entry.ip);
  }
}

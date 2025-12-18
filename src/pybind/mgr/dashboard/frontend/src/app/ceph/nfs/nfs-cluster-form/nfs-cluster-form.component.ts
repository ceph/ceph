import { Component, OnInit } from '@angular/core';
import { FormArray, FormControl, Validators } from '@angular/forms';
import { Router } from '@angular/router';
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

interface HostIpEntry {
  hostname: string;
  ip: string;
}

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
  showDeploymentSettings = false;
  showAdvancedNetwork = false;


  selectedHosts: string[] = [];
  selectedLabels: string[] = [];
  selectedIngressHosts: string[] = [];

  action: string;

  readonly PLACEMENT = {
    host: 'hosts',
    label: 'label'
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
    private orchService: OrchestratorService,
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

    this.createForm();
    this.loadingReady();
  }

  get networks() {
    return this.nfsForm.get('networks') as FormArray;
  }

  get bind_addrs() {
    return this.nfsForm.get('bind_addrs') as FormArray;
  }

  createForm() {
    this.nfsForm = this.formBuilder.group({
      cluster_id: ['', [Validators.required, Validators.pattern(/^[a-z0-9]+(?:-[a-z0-9]+)*$/)]],
      placement: [],
      hosts: [[]],
      label: [
        null,
        [
          CdValidators.requiredIf({
            placement: this.PLACEMENT.label
          })
        ]
      ],
      count: [1, [Validators.min(1)]],
      protocol_version: ['nfsv4'],
      port: [2049, [CdValidators.number(false), Validators.min(1), Validators.max(65535)]],
      networks: this.formBuilder.array([new FormControl('')]),
      bind_addrs: this.formBuilder.array([
        this.formBuilder.group({
          hostname: [''],
          ip: ['']
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
          Validators.pattern(
            /^((25[0-5]|2[0-4]\d|[01]?\d\d?)\.){3}(25[0-5]|2[0-4]\d|[01]?\d\d?)(\/([0-9]|[12]\d|3[0-2]))?$/
          )
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

    this.orchService.status().subscribe((status) => {
      this.hasOrchestrator = status.available;
      this.nfsForm.get('placement').setValue(this.hasOrchestrator ? this.PLACEMENT.host : '');
    });
  }

  addNetwork() {
    this.networks.push(new FormControl(''));
  }

  removeNetwork(index: number) {
    this.networks.removeAt(index);
  }

  addBindAddr() {
    this.bind_addrs.push(
      this.formBuilder.group({
        hostname: [''],
        ip: ['']
      })
    );
  }

  removeBindAddr(index: number) {
    this.bind_addrs.removeAt(index);
  }

  multiSelector(event: any, field: 'label' | 'hosts' | 'ingress_hosts') {
    const values = event.map((item: any) => item.content);
    switch (field) {
      case this.PLACEMENT.host:
        this.selectedHosts = values;
        break;
      case this.PLACEMENT.label:
        this.selectedLabels = values;
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

    const payload: any = {
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
    const placement: any = {};

    if (values.count && values.count > 1) {
      placement.count = values.count;
    }

    switch (values.placement) {
      case this.PLACEMENT.host:
        if (this.selectedHosts.length > 0) {
          placement.hosts = this.selectedHosts;
          placement.count = values.count || this.selectedHosts.length;
        }
        break;
      case this.PLACEMENT.label:
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

  private collectHostIpEntries(entries: any[]): HostIpEntry[] {
    return (entries || [])
      .map((entry: any) => {
        const hostname = typeof entry?.hostname === 'string'
          ? entry.hostname.trim()
          : entry?.hostname?.content?.trim() ?? '';
        const ip = typeof entry?.ip === 'string' ? entry.ip.trim() : '';
        return { hostname, ip };
      })
      .filter((entry: HostIpEntry) => entry.hostname && entry.ip);
  }
}

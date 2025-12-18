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

  selectedHosts: string[] = [];
  selectedLabels: string[] = [];
  selectedIngressHosts: string[] = [];
  selectedIngressLabels: string[] = [];

  action: string;
  resource: string;

  ingressModes = [
    {
      value: 'keepalive-only',
      label: 'keepalive-only',
      helpText: $localize`Virtual IP only; NFS binds directly to the VIP. Only one NFS daemon can be deployed.`
    },
    {
      value: 'haproxy-standard',
      label: 'haproxy-standard',
      helpText: $localize`HAProxy and Keepalived for load balancing. Client IPs are not visible to NFS; IP-based export restrictions will not work.`
    },
    {
      value: 'haproxy-protocol',
      label: 'haproxy-protocol',
      helpText: $localize`HAProxy and Keepalived with PROXY protocol. Client IPs are visible to NFS; IP-based export restrictions work. Requires NFS-Ganesha 5.0 or later.`
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

  get networks() {
    return this.nfsForm.get('networks') as FormArray;
  }

  get bind_addrs() {
    return this.nfsForm.get('bind_addrs') as FormArray;
  }

  get virtual_ips() {
    return this.nfsForm.get('virtual_ips') as FormArray;
  }

  get rdma_networks() {
    return this.nfsForm.get('rdma_networks') as FormArray;
  }

  get monitoring_addrs() {
    return this.nfsForm.get('monitoring_addrs') as FormArray;
  }

  getIngressModeHelpText(): string {
    const selected = this.nfsForm?.get('ingress_mode')?.value;
    return this.ingressModes.find((mode) => mode.value === selected)?.helpText ?? '';
  }

  createForm() {
    this.nfsForm = this.formBuilder.group({
      cluster_id: ['', [Validators.required]],
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
      port: [null, [CdValidators.number(false), Validators.min(1)]],
      networks: this.formBuilder.array([]),
      bind_addrs: this.formBuilder.array([]),
      ingress: [false],
      ingress_placement: ['hosts'],
      ingress_hosts: [[]],
      ingress_label: [
        null,
        [
          CdValidators.requiredIf({
            ingress: true,
            ingress_placement: 'label'
          })
        ]
      ],
      ingress_count: [1, [Validators.min(1)]],
      virtual_ips: this.formBuilder.array([new FormControl('')]),
      ingress_mode: ['haproxy-protocol'],
      enable_nfsv3: [false],
      enable_rdma: [false],
      rdma_port: [null, [CdValidators.number(false), Validators.min(1), Validators.max(65535)]],
      rdma_networks: this.formBuilder.array([]),
      monitoring_addrs: this.formBuilder.array([]),
      monitoring_port: [null, [CdValidators.number(false), Validators.min(1), Validators.max(65535)]]
    });

    this.orchService.status().subscribe((status) => {
      this.hasOrchestrator = status.available;
      this.nfsForm.get('placement').setValue(this.hasOrchestrator ? 'hosts' : '');
    });
  }

  addNetwork() {
    this.networks.push(new FormControl('', Validators.required));
  }

  removeNetwork(index: number) {
    this.networks.removeAt(index);
  }

  addBindAddr() {
    this.bind_addrs.push(
      this.formBuilder.group({
        hostname: ['', Validators.required],
        ip: ['', Validators.required]
      })
    );
  }

  removeBindAddr(index: number) {
    this.bind_addrs.removeAt(index);
  }

  addVirtualIp() {
    this.virtual_ips.push(new FormControl(''));
  }

  removeVirtualIp(index: number) {
    if (this.virtual_ips.length > 1) {
      this.virtual_ips.removeAt(index);
    }
  }

  addRdmaNetwork() {
    this.rdma_networks.push(new FormControl('', Validators.required));
  }

  removeRdmaNetwork(index: number) {
    this.rdma_networks.removeAt(index);
  }

  addMonitoringAddr() {
    this.monitoring_addrs.push(
      this.formBuilder.group({
        hostname: ['', Validators.required],
        ip: ['', Validators.required]
      })
    );
  }

  removeMonitoringAddr(index: number) {
    this.monitoring_addrs.removeAt(index);
  }

  multiSelector(event: any, field: 'label' | 'hosts' | 'ingress_label' | 'ingress_hosts') {
    const values = event.map((item: any) => item.content);
    switch (field) {
      case 'hosts':
        this.selectedHosts = values;
        break;
      case 'label':
        this.selectedLabels = values;
        break;
      case 'ingress_hosts':
        this.selectedIngressHosts = values;
        break;
      case 'ingress_label':
        this.selectedIngressLabels = values;
        break;
    }
  }

  submitAction() {
    this.nfsForm.markAllAsTouched();

    if (this.nfsForm.invalid) {
      return;
    }

    const values = this.nfsForm.getRawValue();
    const virtualIp = (values.virtual_ips || []).map((ip: string) => ip?.trim()).find(Boolean);

    if (values.ingress && !virtualIp) {
      this.virtual_ips.at(0).setErrors({ required: true });
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

    const networks = this.collectNetworks(values);
    if (networks.length > 0) {
      payload.networks = networks;
    }

    const bindAddrs = this.collectHostIpEntries(values.bind_addrs);
    if (bindAddrs.length > 0) {
      payload.bind_addrs = bindAddrs;
    }

    if (values.ingress) {
      payload.virtual_ip = virtualIp;
      if (values.ingress_mode) {
        payload.ingress_mode = values.ingress_mode;
      }
      const ingressPlacementSpec = this.getIngressPlacementSpec(values);
      if (ingressPlacementSpec && Object.keys(ingressPlacementSpec).length > 0) {
        payload.ingress_placement = ingressPlacementSpec;
      }
    }

    if (values.enable_nfsv3) {
      payload.enable_nfsv3 = true;
    }

    if (values.enable_rdma) {
      payload.enable_rdma = true;
      if (values.rdma_port) {
        payload.rdma_port = values.rdma_port;
      }
    }

    const monitoringAddrs = this.collectHostIpEntries(values.monitoring_addrs);
    if (monitoringAddrs.length > 0) {
      payload.monitoring_addrs = monitoringAddrs;
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
      case 'hosts':
        if (this.selectedHosts.length > 0) {
          placement.hosts = this.selectedHosts;
          placement.count = values.count || this.selectedHosts.length;
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

  getIngressPlacementSpec(values: any) {
    const placement: any = {};

    if (values.ingress_count && values.ingress_count > 1) {
      placement.count = values.ingress_count;
    }

    switch (values.ingress_placement) {
      case 'hosts':
        if (this.selectedIngressHosts.length > 0) {
          placement.hosts = this.selectedIngressHosts;
          placement.count = values.ingress_count || this.selectedIngressHosts.length;
        }
        break;
      case 'label':
        if (this.selectedIngressLabels.length > 0) {
          placement.label = this.selectedIngressLabels[0];
          if (values.ingress_count) {
            placement.count = values.ingress_count;
          }
        }
        break;
    }

    return placement;
  }

  private collectNetworks(values: any): string[] {
    const networks = (values.networks || [])
      .map((network: string) => network?.trim())
      .filter(Boolean);
    const rdmaNetworks = values.enable_rdma
      ? (values.rdma_networks || []).map((network: string) => network?.trim()).filter(Boolean)
      : [];
    return [...new Set([...networks, ...rdmaNetworks])];
  }

  private collectHostIpEntries(entries: HostIpEntry[]): HostIpEntry[] {
    return (entries || [])
      .map((entry: HostIpEntry) => ({
        hostname: entry?.hostname?.trim(),
        ip: entry?.ip?.trim()
      }))
      .filter((entry: HostIpEntry) => entry.hostname && entry.ip);
  }
}

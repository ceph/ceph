import { Component, DestroyRef, inject, OnInit } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { FormArray, FormControl, Validators } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';
import { forkJoin, Observable } from 'rxjs';
import { map } from 'rxjs/operators';

import { NfsService } from '~/app/shared/api/nfs.service';
import { HostService } from '~/app/shared/api/host.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { CdForm } from '~/app/shared/forms/cd-form';
import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { ActionLabelsI18n, URLVerbs } from '~/app/shared/constants/app.constants';
import type { NFSBindAddr, NFSCluster, NFSClusterPayload } from '../models/nfs-cluster-config';

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
  showDeploymentSettings = false;
  showAdvancedNetwork = false;
  isEdit = false;
  clusterId!: string;

  selectedHosts: string[] = [];
  selectedIngressHosts: string[] = [];

  action!: string;

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
    private route: ActivatedRoute,
    private hostService: HostService,
    private formBuilder: CdFormBuilder,
    public actionLabels: ActionLabelsI18n
  ) {
    super();
  }

  ngOnInit(): void {
    if (this.route.snapshot.data['editing']) {
      this.isEdit = true;
      this.clusterId = this.route.snapshot.params['cluster_id'];
    }

    this.action = this.isEdit ? this.actionLabels.EDIT : this.actionLabels.CREATE;

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
    if (this.isEdit) {
      this.populateForm();
    } else {
      this.loadingReady();
    }
  }

  private populateForm(): void {
    this.nfsForm.get('cluster_id').disable();
    this.nfsService
      .getCluster(this.clusterId)
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe((cluster: NFSCluster) => {
        this.nfsForm.patchValue({
          cluster_id: cluster.name,
          port: cluster.port ?? 2049,
          ingress: cluster.ingress ?? !!cluster.virtual_ip,
          virtual_ip: cluster.virtual_ip ?? '',
          ingress_mode: cluster.ingress_mode ?? 'haproxy-protocol',
          enable_rdma: cluster.enable_rdma ?? false,
          monitoring_port: cluster.monitor_port
        });

        // Placement
        const p = cluster.placement;
        if (p) {
          if (p.hosts?.length) {
            this.nfsForm.get('placement').setValue(this.PLACEMENT.host);
            this.selectedHosts = p.hosts;
          } else if (p.label) {
            this.nfsForm.get('placement').setValue(this.PLACEMENT.custom);
            this.nfsForm.get('custom_placement_type').setValue(this.CUSTOM_PLACEMENT.label);
            this.nfsForm.get('label').setValue(p.label);
          }
          if (p.count != null) {
            this.nfsForm.get('count').setValue(p.count);
          }
        }

        // Networks
        if (cluster.networks?.length) {
          const networkArray = this.nfsForm.get('networks') as FormArray;
          networkArray.clear();
          cluster.networks.forEach((net) =>
            networkArray.push(new FormControl(net, [CdValidators.ipv4Cidr()]))
          );
          this.showAdvancedNetwork = true;
        }

        // Bind addresses
        if (cluster.bind_addrs?.length) {
          const bindArray = this.nfsForm.get('bind_addrs') as FormArray;
          bindArray.clear();
          cluster.bind_addrs.forEach((addr) =>
            bindArray.push(
              this.formBuilder.group({
                hostname: [{ content: addr.hostname, selected: true }],
                ip: [addr.ip, [CdValidators.ip()]]
              })
            )
          );
          this.showAdvancedNetwork = true;
        }

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
    this.nfsForm = this.formBuilder.group(
      {
      cluster_id: [
        '',
        [Validators.required, Validators.pattern(/^[a-z0-9]+(?:-[a-z0-9]+)*$/)],
        this.isEdit ? [] : [CdValidators.unique((name: string) => this.nfsService.exists(name))]
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
          // Virtual IP is a host address (e.g. 10.20.0.100/24), not a network address,
          // so only the CIDR format is validated — host bits are intentionally non-zero.
          Validators.pattern(
            /^((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\/(([0-9]|[12][0-9]|3[0-2]))$/
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
      }
    );

    this.nfsForm.get('ingress').valueChanges
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe((enabled: boolean) => {
        if (!enabled) {
          this.nfsForm.patchValue({
            virtual_ip: '',
            ingress_mode: 'haproxy-protocol',
            ingress_placement_mode: 'nfs'
          });
          this.selectedIngressHosts = [];
        }
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

    // networks and bind_addrs are incompatible with ingress (NFSServiceSpec enforces this)
    if (!values.ingress) {
      const networks = this.collectNetworks(values);
      if (networks.length > 0) {
        payload.networks = networks;
      }

      const bindAddrs = this.collectHostIpEntries(values.bind_addrs);
      if (bindAddrs.length > 0) {
        payload.bind_addrs = bindAddrs;
      }
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
    } else if (this.isEdit) {
      // Explicitly signal ingress removal so the backend can tear down
      // the ingress service even if virtual_ip was never set in this session.
      payload.ingress = false;
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

    const taskName = this.isEdit ? `nfs/cluster/${URLVerbs.EDIT}` : `nfs/cluster/${URLVerbs.CREATE}`;
    const call = this.isEdit
      ? this.nfsService.updateCluster(this.clusterId, payload)
      : this.nfsService.createCluster(payload);

    this.taskWrapper
      .wrapTaskAroundCall({
        task: new FinishedTask(taskName, { cluster_id: payload.cluster_id }),
        call
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
        let hostname: string;
        if (typeof entry?.hostname === 'string') {
          hostname = entry.hostname.trim();
        } else if (Array.isArray(entry?.hostname)) {
          // multi-select combobox returns an array of { content } objects
          hostname = entry.hostname[0]?.content?.trim() ?? '';
        } else {
          // single-select combobox returns a { content } object
          hostname = entry?.hostname?.content?.trim() ?? '';
        }
        const ip = typeof entry?.ip === 'string' ? entry.ip.trim() : '';
        return { hostname, ip };
      })
      .filter((entry: NFSBindAddr) => entry.hostname && entry.ip);
  }
}

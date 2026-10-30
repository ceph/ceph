import {
  ChangeDetectionStrategy,
  ChangeDetectorRef,
  Component,
  DestroyRef,
  inject,
  OnInit,
  Signal
} from '@angular/core';
import { takeUntilDestroyed, toSignal } from '@angular/core/rxjs-interop';
import {
  AbstractControl,
  FormArray,
  FormControl,
  ValidationErrors,
  Validators
} from '@angular/forms';
import { Router } from '@angular/router';
import { forkJoin, of } from 'rxjs';
import { catchError, map, tap } from 'rxjs/operators';

import { NfsService } from '~/app/shared/api/nfs.service';
import { HostService } from '~/app/shared/api/host.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { CdForm } from '~/app/shared/forms/cd-form';
import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { Host } from '~/app/shared/models/host.interface';
import type { NFSBindAddr, NFSClusterPayload, NFSClusterPlacement } from '../models/nfs.model';
import {
  BindAddrValue,
  ComboBoxContent,
  CustomPlacementType,
  IngressMode,
  NfsClusterFormValue,
  Placement
} from '../nfs.constant';

@Component({
  selector: 'cd-nfs-cluster-form',
  templateUrl: './nfs-cluster-form.component.html',
  standalone: false,
  changeDetection: ChangeDetectionStrategy.OnPush,
  styleUrls: ['./nfs-cluster-form.component.scss']
})
export class NfsClusterFormComponent extends CdForm implements OnInit {
  private destroyRef = inject(DestroyRef);
  private cdr = inject(ChangeDetectorRef);

  nfsForm: CdFormGroup;
  hostsAndLabels: Signal<{ hosts: ComboBoxContent[]; labels: ComboBoxContent[] } | undefined>;
  hostItems: ComboBoxContent[] = [];
  labelItems: ComboBoxContent[] = [];
  clusterNames: string[] = [];
  private _showDeploymentSettings = false;
  get showDeploymentSettings(): boolean {
    return this._showDeploymentSettings;
  }
  set showDeploymentSettings(value: boolean) {
    this._showDeploymentSettings = value;
    this.cdr.markForCheck();
  }
  showAdvancedNetwork = false;

  selectedHosts: string[] = [];
  selectedIngressHosts: string[] = [];

  action: string;

  INVALID_TEXTS = {
    required: $localize`This field is required.`,
    clusterIdPattern: $localize`Use lowercase letters, numbers, and hyphens. Cannot start or end with a hyphen.`,
    uniqueName: $localize`A cluster with this name already exists.`,
    min: $localize`The value must be at least 1.`,
    max: $localize`The value cannot exceed 65535.`,
    invalidNetwork: $localize`Enter a network address with the host bits cleared (e.g. 10.20.0.0/24).`,
    invalidIp: $localize`Enter a valid IPv4 or IPv6 address.`,
    invalidCidr: $localize`Enter a valid IPv4 address, optionally with a prefix length (e.g. 10.20.0.100/24).`
  };

  readonly PLACEMENT = Placement;
  readonly CUSTOM_PLACEMENT_RADIO = CustomPlacementType;

  readonly INGRESS_MODES = [
    {
      value: IngressMode.HaproxyProtocol,
      label: $localize`Balance traffic across different IPs (HAProxy protocol)`
    },
    {
      value: IngressMode.HaproxyStandard,
      label: $localize`Balance traffic only (HAProxy standard)`
    },
    {
      value: IngressMode.KeepaliveOnly,
      label: $localize`Failover only (Keepalive only)`
    }
  ];

  readonly INGRESS_PLACEMENT_MODES = [
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
    this.hostsAndLabels = toSignal(
      forkJoin({
        hosts: this.hostService.getAllHosts(),
        labels: this.hostService.getLabels()
      }).pipe(
        map(({ hosts, labels }) => ({
          hosts: hosts.map((host: Host) => ({
            content: host.hostname,
            selected: hosts.length === 1
          })),
          labels: labels.map((label: string) => ({
            content: label,
            selected: labels.length === 1
          }))
        })),
        tap((data) => {
          this.hostItems = data.hosts;
          this.labelItems = data.labels;
          this.preselectSingleOptions();
        })
      )
    );
  }

  ngOnInit(): void {
    this.action = this.actionLabels.CREATE;

    this.nfsService
      .listClusters(false)
      .pipe(
        catchError(() => of([])),
        takeUntilDestroyed(this.destroyRef)
      )
      .subscribe((names) => {
        this.clusterNames = names as string[];
        this.createForm();
        this.loadingReady();
        this.cdr.markForCheck();
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
          CdValidators.custom(
            'clusterIdPattern',
            (id: string) => !!id && !/^[a-z0-9]+(?:-[a-z0-9]+)*$/.test(id)
          ),
          CdValidators.custom('uniqueName', (id: string) => this.clusterNames?.includes(id))
        ]
      ],
      placement: [Placement.Auto],
      hosts: [[]],
      label: [
        null,
        [
          CdValidators.requiredIf({
            placement: Placement.Custom,
            custom_placement_type: CustomPlacementType.Label
          })
        ]
      ],
      custom_placement_type: [CustomPlacementType.Label],
      host_pattern: [
        '',
        [
          CdValidators.requiredIf({
            placement: Placement.Custom,
            custom_placement_type: CustomPlacementType.HostPattern
          })
        ]
      ],
      count: [1, [Validators.min(1)]],
      protocol_version: ['nfsv4'],
      port: [2049, [CdValidators.number(false), Validators.min(1), Validators.max(65535)]],
      networks: this.formBuilder.array([new FormControl('', [this.invalidNetwork])]),
      bind_addrs: this.formBuilder.array([
        this.formBuilder.group({
          hostname: [''],
          ip: ['', [this.invalidIp]]
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
          this.invalidCidr
        ]
      ],
      ingress_mode: [IngressMode.HaproxyProtocol],
      enable_rdma: [false],
      rdma_port: [20049, [CdValidators.number(false), Validators.min(1), Validators.max(65535)]],
      monitoring_port: [
        9587,
        [CdValidators.number(false), Validators.min(1), Validators.max(65535)]
      ]
    });
    this.preselectSingleOptions();

    for (const controlName of [
      'ingress',
      'placement',
      'custom_placement_type',
      'ingress_mode',
      'ingress_placement_mode'
    ]) {
      this.nfsForm
        .get(controlName)
        .valueChanges.pipe(takeUntilDestroyed(this.destroyRef))
        .subscribe(() => this.cdr.markForCheck());
    }
  }

  private singleHost(): ComboBoxContent | null {
    return this.hostItems.length === 1 ? this.hostItems[0] : null;
  }

  private preselectSingleOptions() {
    if (!this.nfsForm) {
      return;
    }

    const host = this.singleHost();
    if (host) {
      host.selected = true;
      const hostsCtrl = this.nfsForm.get('hosts');
      if (!hostsCtrl.value?.length) {
        hostsCtrl.setValue([host]);
        this.selectedHosts = [host.content];
      }

      const ingressHostsCtrl = this.nfsForm.get('ingress_hosts');
      if (!ingressHostsCtrl.value?.length) {
        ingressHostsCtrl.setValue([host]);
        this.selectedIngressHosts = [host.content];
      }

      this.bind_addrs.controls.forEach((group) => {
        const hostnameCtrl = group.get('hostname');
        if (hostnameCtrl && !hostnameCtrl.value) {
          hostnameCtrl.setValue(host);
        }
      });
    }

    if (this.labelItems.length === 1) {
      const label = this.labelItems[0];
      label.selected = true;
      const labelCtrl = this.nfsForm.get('label');
      if (!labelCtrl.value) {
        labelCtrl.setValue(label);
      }
    }
  }

  addNetwork() {
    this.networks.push(new FormControl('', [this.invalidNetwork]));
  }

  removeNetwork(index: number) {
    this.networks.removeAt(index);
  }

  addBindAddr() {
    this.bind_addrs.push(
      this.formBuilder.group({
        hostname: [this.singleHost() ?? ''],
        ip: ['', [this.invalidIp]]
      })
    );
  }

  removeBindAddr(index: number) {
    this.bind_addrs.removeAt(index);
  }

  multiSelector(event: ComboBoxContent[], field: Placement.Host | Placement.IngressHost) {
    const values = event.map((item) => item.content);
    switch (field) {
      case Placement.Host:
        this.selectedHosts = values;
        break;
      case Placement.IngressHost:
        this.selectedIngressHosts = values;
        break;
    }
  }

  submitAction() {
    this.nfsForm.markAllAsTouched();

    if (this.nfsForm.invalid) {
      return;
    }

    const values: NfsClusterFormValue = this.nfsForm.getRawValue();
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

    const networkIncompatibleMode =
      values.ingress && values.ingress_mode === IngressMode.KeepaliveOnly;
    if (!networkIncompatibleMode) {
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

  getPlacementSpec(values: NfsClusterFormValue): NFSClusterPlacement {
    const placement: NFSClusterPlacement = { count: values.count };

    switch (values.placement) {
      case Placement.Host:
        if (this.selectedHosts.length > 0) {
          placement.hosts = this.selectedHosts;
        }
        break;
      case Placement.Custom:
        if (values.custom_placement_type === CustomPlacementType.Label && values.label) {
          placement.label = typeof values.label === 'string' ? values.label : values.label.content;
        } else if (
          values.custom_placement_type === CustomPlacementType.HostPattern &&
          values.host_pattern
        ) {
          placement.host_pattern = values.host_pattern;
        }
        break;
    }

    return placement;
  }

  getIngressPlacementSpec(): NFSClusterPlacement {
    const placement: NFSClusterPlacement = {};
    if (this.selectedIngressHosts.length > 0) {
      placement.hosts = this.selectedIngressHosts;
      placement.count = this.selectedIngressHosts.length;
    }
    return placement;
  }

  private collectNetworks(values: NfsClusterFormValue): string[] {
    const networks = (values.networks || [])
      .map((network) => network?.trim())
      .filter((network): network is string => !!network);
    return [...new Set(networks)];
  }

  private ipValidator = CdValidators.ip();
  private cidrValidator = CdValidators.ipv4Cidr();

  private invalidIp = (control: AbstractControl): ValidationErrors | null =>
    this.ipValidator(control) ? { invalidIp: true } : null;

  private invalidCidr = (control: AbstractControl): ValidationErrors | null =>
    this.cidrValidator(control) ? { invalidCidr: true } : null;

  private invalidNetwork = (control: AbstractControl): ValidationErrors | null =>
    this.cidrValidator(control) || this.noHostBits(control) ? { invalidNetwork: true } : null;

  private noHostBits = (control: AbstractControl): ValidationErrors | null => {
    const value = String(control.value ?? '');
    if (!control.value || value.indexOf('/') === -1) {
      return null;
    }
    const [addr, prefixText] = value.split('/');
    const prefix = Number(prefixText);
    if (!Number.isInteger(prefix) || prefix < 0 || prefix > 32) {
      return null;
    }
    const ip = addr.split('.').reduce((acc, octet) => acc * 256 + Number(octet), 0);
    return ip % 2 ** (32 - prefix) === 0 ? null : { pattern: true };
  };

  private collectHostIpEntries(entries: BindAddrValue[]): NFSBindAddr[] {
    return (entries || [])
      .map((entry) => {
        const hostname =
          typeof entry.hostname === 'string'
            ? entry.hostname.trim()
            : (entry.hostname?.content?.trim() ?? '');
        const ip = entry.ip?.trim() ?? '';
        return { hostname, ip };
      })
      .filter((entry) => entry.hostname && entry.ip);
  }
}

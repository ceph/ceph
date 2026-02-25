import { Component, OnInit } from '@angular/core';
import { UntypedFormControl, Validators } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';
import {
  NamespaceCreateRequest,
  NamespaceInitiatorRequest,
  NvmeofService
} from '~/app/shared/api/nvmeof.service';
import { ActionLabelsI18n, URLVerbs } from '~/app/shared/constants/app.constants';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { FinishedTask } from '~/app/shared/models/finished-task';
import {
  NvmeofSubsystem,
  NvmeofSubsystemInitiator,
  NvmeofSubsystemNamespace
} from '~/app/shared/models/nvmeof';
import { Permission } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { Pool } from '../../pool/pool';
import { PoolService } from '~/app/shared/api/pool.service';
import { RbdService } from '~/app/shared/api/rbd.service';
import { FormatterService } from '~/app/shared/services/formatter.service';
import { forkJoin, Observable, of } from 'rxjs';
import { switchMap } from 'rxjs/operators';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { DimlessBinaryPipe } from '~/app/shared/pipes/dimless-binary.pipe';
import { HttpResponse } from '@angular/common/http';

@Component({
  selector: 'cd-nvmeof-namespaces-form',
  templateUrl: './nvmeof-namespaces-form.component.html',
  styleUrls: ['./nvmeof-namespaces-form.component.scss'],
  standalone: false
})
export class NvmeofNamespacesFormComponent implements OnInit {
  action: string;
  permission: Permission;
  poolPermission: Permission;
  resource: string;
  pageURL: string;

  nsForm: CdFormGroup;
  subsystemNQN: string;
  subsystems: NvmeofSubsystem[];
  rbdPools: Array<Pool> = null;
  rbdImages: any[] = [];
  initiatorCandidates: { content: string; selected: boolean }[] = [];

  nsid: string;

  group: string;
  MAX_NAMESPACE_CREATE: number = 5;
  MIN_NAMESPACE_CREATE: number = 1;
  requiredInvalidText: string = $localize`This field is required`;
  nsCountInvalidText: string = $localize`The namespace count should be between 1 and 5`;

  constructor(
    public actionLabels: ActionLabelsI18n,
    private authStorageService: AuthStorageService,
    private taskWrapperService: TaskWrapperService,
    private nvmeofService: NvmeofService,
    private poolService: PoolService,
    private rbdService: RbdService,
    private router: Router,
    private route: ActivatedRoute,
    public formatterService: FormatterService,
    public dimlessBinaryPipe: DimlessBinaryPipe
  ) {
    this.permission = this.authStorageService.getPermissions().nvmeof;
    this.poolPermission = this.authStorageService.getPermissions().pool;
    this.resource = $localize`Namespace`;
    this.pageURL = 'block/nvmeof/gateways';
  }

  init() {
    this.route.queryParams.subscribe((params) => {
      this.group = params?.['group'];
    });
    this.createForm();
    this.action = this.actionLabels.CREATE;
    this.route.params.subscribe((params: { subsystem_nqn: string; nsid: string }) => {
      this.subsystemNQN = params.subsystem_nqn;
      this.nsid = params?.nsid;
    });
    this.route.queryParams.subscribe((params) => {
      if (params?.['subsystem_nqn']) {
        this.subsystemNQN = params?.['subsystem_nqn'];
      }
    });
  }

  initForCreate() {
    this.poolService.getList().subscribe((resp: Pool[]) => {
      this.rbdPools = resp.filter(this.rbdService.isRBDPool);
    });
    if (this.group) {
      this.fetchUsedImages();
      this.nvmeofService.listSubsystems(this.group).subscribe((subsystems: NvmeofSubsystem[]) => {
        this.subsystems = subsystems;
        if (this.subsystemNQN) {
          const selectedSubsystem = this.subsystems.find((s) => s.nqn === this.subsystemNQN);
          if (selectedSubsystem) {
            this.nsForm.get('subsystem').setValue(selectedSubsystem.nqn);
          }
        }
      });
    }
  }

  ngOnInit() {
    this.init();
    this.initForCreate();
    const subsystemControl = this.nsForm.get('subsystem');
    if (subsystemControl) {
      subsystemControl.valueChanges.subscribe((nqn: string) => {
        this.onSubsystemChange(nqn);
      });
    }
  }

  // Stores all RBD images fetched for the selected pool
  private allRbdImages: { name: string; size: number }[] = [];
  // Maps pool name to a Set of used image names for O(1) lookup
  private usedRbdImages: Map<string, Set<string>> = new Map();

  onPoolChange(): void {
    const pool = this.nsForm.getValue('pool');
    if (!pool) return;

    this.rbdService
      .list({ pool_name: pool, offset: '0', limit: '-1' })
      .subscribe((pools: { pool_name: string; value: { name: string; size: number }[] }[]) => {
        const selectedPool = pools.find((p) => p.pool_name === pool);
        this.allRbdImages = selectedPool?.value ?? [];
        this.filterImages();

        const imageControl = this.nsForm.get('rbd_image_name');
        const currentImage = this.nsForm.getValue('rbd_image_name');
        if (currentImage && !this.rbdImages.some((img) => img.name === currentImage)) {
          imageControl.setValue(null);
        }
        imageControl.markAsUntouched();
        imageControl.markAsPristine();
      });
  }

  fetchUsedImages(): void {
    if (!this.group) return;

    this.nvmeofService.listNamespaces(this.group).subscribe((response: any) => {
      const namespaces: NvmeofSubsystemNamespace[] = Array.isArray(response)
        ? response
        : response?.namespaces ?? [];
      this.usedRbdImages = namespaces.reduce((map, ns) => {
        if (!map.has(ns.rbd_pool_name)) {
          map.set(ns.rbd_pool_name, new Set<string>());
        }
        map.get(ns.rbd_pool_name)!.add(ns.rbd_image_name);
        return map;
      }, new Map<string, Set<string>>());
      this.filterImages();
    });
  }

  onSubsystemChange(nqn: string): void {
    if (!nqn) return;
    this.nvmeofService
      .getInitiators(nqn, this.group)
      .subscribe((response: NvmeofSubsystemInitiator[] | { hosts: NvmeofSubsystemInitiator[] }) => {
        const initiators = Array.isArray(response) ? response : response?.hosts || [];
        this.initiatorCandidates = initiators.map((initiator) => ({
          content: initiator.nqn,
          selected: false
        }));
      });
  }

  onInitiatorSelection(event: any) {
    // Carbon ComboBox (selected) emits the full array of selected items
    const selectedInitiators = Array.isArray(event) ? event.map((e: any) => e.content) : [];
    this.nsForm
      .get('initiators')
      .setValue(selectedInitiators.length > 0 ? selectedInitiators : null);
    this.nsForm.get('initiators').markAsDirty();
    this.nsForm.get('initiators').markAsTouched();
  }

  private filterImages(): void {
    const pool = this.nsForm.getValue('pool');
    if (!pool) {
      this.rbdImages = [];
      return;
    }
    const usedInPool = this.usedRbdImages.get(pool);
    this.rbdImages = usedInPool
      ? this.allRbdImages.filter((img) => !usedInPool.has(img.name))
      : [...this.allRbdImages];
  }

  createForm() {
    this.nsForm = new CdFormGroup({
      pool: new UntypedFormControl('', {
        validators: [Validators.required]
      }),
      subsystem: new UntypedFormControl('', {
        validators: [Validators.required]
      }),
      image_size: new UntypedFormControl(null, {
        validators: [Validators.required]
      }),
      nsCount: new UntypedFormControl(this.MAX_NAMESPACE_CREATE, [
        Validators.required,
        Validators.max(this.MAX_NAMESPACE_CREATE),
        Validators.min(this.MIN_NAMESPACE_CREATE)
      ]),
      rbd_image_creation: new UntypedFormControl('gateway_provisioned'),

      rbd_image_name: new UntypedFormControl(null, [
        CdValidators.custom('rbdImageName', (value: any) => {
          if (!value) return null;
          return /^[^@/]+$/.test(value) ? null : { rbdImageName: true };
        })
      ]),
      namespace_size: new UntypedFormControl(null, {
        validators: [CdValidators.blockSizeMultiple()]
      }), // UI only - not sent to backend
      host_access: new UntypedFormControl('all'), // UI only - determines visibility
      initiators: new UntypedFormControl([]) // UI only - selected hosts
    });

    this.nsForm.get('pool').valueChanges.subscribe(() => {
      this.onPoolChange();
    });

    this.nsForm.get('nsCount').valueChanges.subscribe((count: number) => {
      if (count > 1) {
        const creationControl = this.nsForm.get('rbd_image_creation');
        if (creationControl.value === 'externally_managed') {
          creationControl.setValue('gateway_provisioned');
        }
      }
    });

    this.nsForm.get('rbd_image_creation').valueChanges.subscribe((mode: string) => {
      const nameControl = this.nsForm.get('rbd_image_name');
      const sizeControl = this.nsForm.get('image_size');
      const countControl = this.nsForm.get('nsCount');

      if (mode === 'externally_managed') {
        countControl.setValue(1);
        countControl.disable();
        this.onPoolChange();
        nameControl.addValidators(Validators.required);
        sizeControl.removeValidators(Validators.required);
        sizeControl.disable();
      } else {
        sizeControl.enable();
        countControl.enable();
        nameControl.removeValidators(Validators.required);
        sizeControl.addValidators(Validators.required);
      }
      nameControl.updateValueAndValidity();
      sizeControl.updateValueAndValidity();
    });

    this.nsForm.get('host_access').valueChanges.subscribe((mode: string) => {
      const initiatorsControl = this.nsForm.get('initiators');
      if (mode === 'specific') {
        initiatorsControl.addValidators(Validators.required);
      } else {
        initiatorsControl.removeValidators(Validators.required);
        initiatorsControl.setValue([]);
        this.initiatorCandidates.forEach((i) => (i.selected = false));
      }
      initiatorsControl.updateValueAndValidity();
    });
  }

  randomString() {
    return Math.random().toString(36).substring(2);
  }

  private normalizeImageSizeInput(value: string): string {
    const input = (value || '').trim();
    if (!input) {
      return input;
    }
    // Accept plain numeric values as GiB (e.g. "45" => "45GiB").
    return /^\d+(\.\d+)?$/.test(input) ? `${input}GiB` : input;
  }

  buildCreateRequest(
    rbdImageSize: number,
    nsCount: number,
    noAutoVisible: boolean
  ): Observable<HttpResponse<Object>>[] {
    const pool = this.nsForm.getValue('pool');
    const requests: Observable<HttpResponse<Object>>[] = [];
    const creationMode = this.nsForm.getValue('rbd_image_creation');
    const isGatewayProvisioned = creationMode === 'gateway_provisioned';

    const loopCount = isGatewayProvisioned ? nsCount : 1;

    for (let i = 1; i <= loopCount; i++) {
      const request: NamespaceCreateRequest = {
        gw_group: this.group,
        rbd_pool: pool,
        create_image: isGatewayProvisioned,
        no_auto_visible: noAutoVisible
      };

      const blockSize = this.nsForm.getValue('namespace_size');
      if (blockSize) {
        request.block_size = blockSize;
      }

      if (isGatewayProvisioned) {
        request.rbd_image_name = `nvme_${pool}_${this.group}_${this.randomString()}`;
        if (rbdImageSize) {
          request['rbd_image_size'] = rbdImageSize;
        }
      }

      const rbdImageName = this.nsForm.getValue('rbd_image_name');
      if (rbdImageName) {
        request['rbd_image_name'] = rbdImageName;
      }

      const subsystemNQN = this.nsForm.getValue('subsystem') || this.subsystemNQN;
      requests.push(this.nvmeofService.createNamespace(subsystemNQN, request));
    }

    return requests;
  }

  onSubmit() {
    if (this.nsForm.invalid) {
      this.nsForm.setErrors({ cdSubmitButton: true });
      this.nsForm.markAllAsTouched();
      return;
    }

    const component = this;
    const taskUrl: string = `nvmeof/namespace/${URLVerbs.CREATE}`;
    const image_size = this.nsForm.getValue('image_size');
    const nsCount = this.nsForm.getValue('nsCount');
    const hostAccess = this.nsForm.getValue('host_access');
    const selectedHosts: string[] = this.nsForm.getValue('initiators') || [];
    const noAutoVisible = hostAccess === 'specific';
    let action: Observable<any>;
    let rbdImageSize: number = null;

    if (image_size) {
      const normalizedSize = this.normalizeImageSizeInput(image_size);
      rbdImageSize = this.formatterService.toBytes(normalizedSize);
      if (rbdImageSize === null) {
        this.nsForm.get('image_size').setErrors({ invalid: true });
        this.nsForm.setErrors({ cdSubmitButton: true });
        return;
      }
    }

    const subsystemNQN = this.nsForm.getValue('subsystem');

    // Step 1: Create namespaces
    // Step 2: If specific hosts selected, chain addNamespaceInitiators calls
    const createObs = forkJoin(this.buildCreateRequest(rbdImageSize, nsCount, noAutoVisible));

    const combinedObs = createObs.pipe(
      switchMap((responses: HttpResponse<Object>[]) => {
        if (noAutoVisible && selectedHosts.length > 0) {
          const initiatorObs: Observable<any>[] = [];

          responses.forEach((res) => {
            const body: any = res.body;
            if (body && body.nsid) {
              selectedHosts.forEach((host: string) => {
                const req: NamespaceInitiatorRequest = {
                  gw_group: this.group,
                  subsystem_nqn: subsystemNQN || this.subsystemNQN,
                  host_nqn: host
                };
                initiatorObs.push(this.nvmeofService.addNamespaceInitiators(body.nsid, req));
              });
            }
          });

          if (initiatorObs.length > 0) {
            return forkJoin(initiatorObs);
          }
        }
        return of(responses);
      })
    );

    action = this.taskWrapperService.wrapTaskAroundCall({
      task: new FinishedTask(taskUrl, {
        nqn: subsystemNQN,
        nsCount
      }),
      call: combinedObs
    });

    action.subscribe({
      error: () => {
        component.nsForm.setErrors({ cdSubmitButton: true });
      },
      complete: () => {
        this.router.navigate([this.pageURL], {
          queryParams: { group: this.group, tab: 'namespace' }
        });
      }
    });
  }
}

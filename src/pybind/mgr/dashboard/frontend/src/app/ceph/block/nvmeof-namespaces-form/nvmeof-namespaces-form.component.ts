import { Component, OnDestroy, OnInit } from '@angular/core';
import { UntypedFormControl, Validators } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';
import {
  NamespaceCreateRequest,
  NamespaceInitiatorRequest,
  NamespaceUpdateRequest,
  NvmeofService
} from '~/app/shared/api/nvmeof.service';
import { ActionLabelsI18n, URLVerbs } from '~/app/shared/constants/app.constants';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { FinishedTask } from '~/app/shared/models/finished-task';
import {
  NvmeofSubsystem,
  NvmeofSubsystemInitiator,
  NvmeofSubsystemNamespace,
  NvmeofNamespaceListResponse,
  NvmeofInitiatorCandidate,
  NsFormField,
  RbdImageCreation,
  HOST_TYPE
} from '~/app/shared/models/nvmeof';
import { Permission } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { Pool } from '../../pool/pool';
import { PoolService } from '~/app/shared/api/pool.service';
import { RbdPool, RbdImage } from '~/app/shared/api/rbd.model';
import { RbdService } from '~/app/shared/api/rbd.service';
import { FormatterService } from '~/app/shared/services/formatter.service';
import { forkJoin, Observable, of, Subject } from 'rxjs';
import { filter, switchMap, takeUntil, tap } from 'rxjs/operators';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { DimlessBinaryPipe } from '~/app/shared/pipes/dimless-binary.pipe';
import { HttpResponse } from '@angular/common/http';

@Component({
  selector: 'cd-nvmeof-namespaces-form',
  templateUrl: './nvmeof-namespaces-form.component.html',
  styleUrls: ['./nvmeof-namespaces-form.component.scss']
})
export class NvmeofNamespacesFormComponent implements OnInit, OnDestroy {
  action: string;
  permission: Permission;
  poolPermission: Permission;
  resource: string;
  pageURL: string;
  edit: boolean = false;
  nsForm: CdFormGroup;
  subsystemNQN: string;
  subsystems?: NvmeofSubsystem[];
  rbdPools: Pool[] | null = null;
  rbdImages: RbdImage[] = [];
  initiatorCandidates: NvmeofInitiatorCandidate[] = [];

  // Stores all RBD images fetched for the selected pool
  private allRbdImages: RbdImage[] = [];
  // Maps pool name to a Set of used image names for O(1) lookup
  private usedRbdImages: Map<string, Set<string>> = new Map();
  private lastSubsystemNqn: string;

  nsid: string;
  currentBytes: number = 0;
  group: string;
  MAX_NAMESPACE_CREATE: number = 5;
  MIN_NAMESPACE_CREATE: number = 1;
  private destroy$ = new Subject<void>();
  INVALID_TEXTS: Record<string, string> = {
    required: $localize`This field is required.`,
    min: $localize`The namespace count should be between 1 and 5.`,
    max: $localize`The namespace count should be between 1 and 5.`,
    minSize: $localize`Enter a value larger than previous. A block device image can be expanded but not reduced.`,
    rbdImageName: $localize`Image name contains invalid characters.`
  };

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

  ngOnDestroy() {
    this.destroy$.next();
    this.destroy$.complete();
  }

  init() {
    this.route.queryParams.subscribe((params) => {
      this.group = params?.['group'];
      if (params?.['subsystem_nqn']) {
        this.subsystemNQN = params?.['subsystem_nqn'];
      }
    });

    this.createForm();
    this.action = this.actionLabels.CREATE;
    this.route.params.subscribe((params: { subsystem_nqn: string; nsid: string }) => {
      this.subsystemNQN = params.subsystem_nqn;
      this.nsid = params?.nsid;
    });
  }

  initForEdit() {
    this.edit = true;
    this.action = this.actionLabels.EDIT;
    this.nvmeofService
      .getNamespace(this.subsystemNQN, this.nsid, this.group)
      .subscribe((res: NvmeofSubsystemNamespace) => {
        this.currentBytes =
          typeof res.rbd_image_size === 'string' ? Number(res.rbd_image_size) : res.rbd_image_size;
        this.nsForm.get(NsFormField.POOL).setValue(res.rbd_pool_name);
        this.nsForm
          .get(NsFormField.IMAGE_SIZE)
          .setValue(this.dimlessBinaryPipe.transform(res.rbd_image_size));
        this.nsForm.get(NsFormField.IMAGE_SIZE).addValidators(Validators.required);
        this.nsForm.get(NsFormField.POOL).disable();
        this.nsForm.get(NsFormField.SUBSYSTEM).disable();
        this.nsForm.get(NsFormField.SUBSYSTEM).setValue(this.subsystemNQN);
      });
  }

  initForCreate() {
    this.poolService.getList().subscribe((resp: Pool[]) => {
      this.rbdPools = resp.filter(this.rbdService.isRBDPool);
    });
    this.route.queryParams
      .pipe(
        filter((params) => params?.['group']),
        tap((params) => {
          this.group = params['group'];
          this.fetchUsedImages();
        }),
        switchMap(() => this.nvmeofService.listSubsystems(this.group))
      )
      .subscribe((subsystems: NvmeofSubsystem[]) => {
        this.subsystems = subsystems;
        if (this.subsystemNQN) {
          const selectedSubsystem = this.subsystems.find((s) => s.nqn === this.subsystemNQN);
          if (selectedSubsystem) {
            this.nsForm.get(NsFormField.SUBSYSTEM).setValue(selectedSubsystem.nqn);
          }
        }
      });
  }

  ngOnInit() {
    this.init();
    if (this.router.url.includes('subsystems/(modal:edit')) {
      this.initForEdit();
    } else {
      this.initForCreate();
    }
    const subsystemControl = this.nsForm.get(NsFormField.SUBSYSTEM);
    if (subsystemControl) {
      subsystemControl.valueChanges.subscribe((nqn: string) => {
        this.onSubsystemChange(nqn);
      });
    }
  }

  onPoolChange(): void {
    const pool = this.nsForm.getValue(NsFormField.POOL);
    if (!pool) return;

    this.rbdService
      .list({ pool_name: pool, offset: '0', limit: '-1' })
      .subscribe((pools: RbdPool[]) => {
        const selectedPool = pools.find((p) => p.pool_name === pool);
        this.allRbdImages = selectedPool?.value ?? [];
        this.filterImages();

        const imageControl = this.nsForm.get(NsFormField.RBD_IMAGE_NAME);
        const currentImage = this.nsForm.getValue(NsFormField.RBD_IMAGE_NAME);
        if (currentImage && !this.rbdImages.some((img) => img.name === currentImage)) {
          imageControl.setValue(null);
        }
        imageControl.markAsUntouched();
        imageControl.markAsPristine();
      });
  }

  fetchUsedImages(): void {
    if (!this.group) return;

    this.nvmeofService
      .listNamespaces(this.group)
      .subscribe((response: NvmeofNamespaceListResponse) => {
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
    if (!nqn || nqn === this.lastSubsystemNqn) return;
    this.lastSubsystemNqn = nqn;
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

  onInitiatorSelection(event: NvmeofInitiatorCandidate[]) {
    // Carbon ComboBox (selected) emits the full array of selected items
    const selectedInitiators = Array.isArray(event) ? event.map((e) => e.content) : [];
    this.nsForm
      .get(NsFormField.INITIATORS)
      .setValue(selectedInitiators.length > 0 ? selectedInitiators : null);
    this.nsForm.get(NsFormField.INITIATORS).markAsDirty();
    this.nsForm.get(NsFormField.INITIATORS).markAsTouched();
  }

  private filterImages(): void {
    const pool = this.nsForm.getValue(NsFormField.POOL);
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
      [NsFormField.POOL]: new UntypedFormControl('', {
        validators: [Validators.required]
      }),
      [NsFormField.SUBSYSTEM]: new UntypedFormControl('', {
        validators: [Validators.required]
      }),
      [NsFormField.IMAGE_SIZE]: new UntypedFormControl(null, {
        validators: [
          Validators.required,
          CdValidators.custom('minSize', (value: any) => {
            if (value !== null && value !== undefined && value !== '') {
              const bytes = this.formatterService.toBytes(value);
              if (
                (!this.edit && bytes <= 0) ||
                (this.edit && this.currentBytes && bytes <= this.currentBytes)
              ) {
                return { minSize: true };
              }
            }
            return null;
          })
        ],
        updateOn: 'blur'
      }),
      [NsFormField.NS_COUNT]: new UntypedFormControl(this.MAX_NAMESPACE_CREATE, [
        Validators.required,
        Validators.max(this.MAX_NAMESPACE_CREATE),
        Validators.min(this.MIN_NAMESPACE_CREATE)
      ]),
      [NsFormField.RBD_IMAGE_CREATION]: new UntypedFormControl(
        RbdImageCreation.GATEWAY_PROVISIONED
      ),

      [NsFormField.RBD_IMAGE_NAME]: new UntypedFormControl(null, [
        CdValidators.custom('rbdImageName', (value: any) => {
          if (!value) return null;
          return /^[^@/]+$/.test(value) ? null : { rbdImageName: true };
        })
      ]),
      [NsFormField.NAMESPACE_SIZE]: new UntypedFormControl(null, [Validators.min(0)]), // sent as block_size in create request
      [NsFormField.HOST_ACCESS]: new UntypedFormControl(HOST_TYPE.ALL), // drives no_auto_visible in create request
      [NsFormField.INITIATORS]: new UntypedFormControl([]) // sent via addNamespaceInitiators API
    });

    this.nsForm
      .get(NsFormField.POOL)
      .valueChanges.pipe(takeUntil(this.destroy$))
      .subscribe(() => {
        this.onPoolChange();
      });

    this.nsForm
      .get(NsFormField.NS_COUNT)
      .valueChanges.pipe(takeUntil(this.destroy$))
      .subscribe((count: number) => {
        if (count > 1) {
          const creationControl = this.nsForm.get(NsFormField.RBD_IMAGE_CREATION);
          if (creationControl.value === RbdImageCreation.EXTERNALLY_MANAGED) {
            creationControl.setValue(RbdImageCreation.GATEWAY_PROVISIONED);
          }
        }
      });

    this.nsForm
      .get(NsFormField.RBD_IMAGE_CREATION)
      .valueChanges.pipe(takeUntil(this.destroy$))
      .subscribe((mode: string) => {
        const nameControl = this.nsForm.get(NsFormField.RBD_IMAGE_NAME);
        const countControl = this.nsForm.get(NsFormField.NS_COUNT);
        const imageSizeControl = this.nsForm.get(NsFormField.IMAGE_SIZE);

        if (mode === RbdImageCreation.EXTERNALLY_MANAGED) {
          countControl.setValue(1);
          countControl.disable();
          this.onPoolChange();
          nameControl.addValidators(Validators.required);
          imageSizeControl.disable();
          imageSizeControl.removeValidators(Validators.required);
        } else {
          countControl.enable();
          nameControl.removeValidators(Validators.required);
          imageSizeControl.enable();
          imageSizeControl.addValidators(Validators.required);
        }
        nameControl.updateValueAndValidity();
        imageSizeControl.updateValueAndValidity();
      });

    this.nsForm
      .get(NsFormField.HOST_ACCESS)
      .valueChanges.pipe(takeUntil(this.destroy$))
      .subscribe((mode: string) => {
        const initiatorsControl = this.nsForm.get(NsFormField.INITIATORS);
        if (mode === HOST_TYPE.SPECIFIC) {
          initiatorsControl.addValidators(Validators.required);
        } else {
          initiatorsControl.removeValidators(Validators.required);
          initiatorsControl.setValue([]);
          this.initiatorCandidates.forEach((i) => (i.selected = false));
        }
        initiatorsControl.updateValueAndValidity();
      });
  }

  buildUpdateRequest(rbdImageSize: number): Observable<HttpResponse<Object>> {
    const request: NamespaceUpdateRequest = {
      gw_group: this.group,
      rbd_image_size: rbdImageSize
    };
    return this.nvmeofService.updateNamespace(
      this.subsystemNQN,
      this.nsid,
      request as NamespaceUpdateRequest
    );
  }

  randomString() {
    return Math.random().toString(36).substring(2);
  }

  buildCreateRequest(
    rbdImageSize: number,
    nsCount: number,
    noAutoVisible: boolean
  ): Observable<HttpResponse<Object>>[] {
    const pool = this.nsForm.getValue(NsFormField.POOL);
    const requests: Observable<HttpResponse<Object>>[] = [];
    const creationMode = this.nsForm.getValue(NsFormField.RBD_IMAGE_CREATION);
    const isGatewayProvisioned = creationMode === RbdImageCreation.GATEWAY_PROVISIONED;

    const loopCount = isGatewayProvisioned ? nsCount : 1;

    for (let i = 1; i <= loopCount; i++) {
      const request: NamespaceCreateRequest = {
        gw_group: this.group,
        rbd_pool: pool,
        create_image: isGatewayProvisioned,
        no_auto_visible: noAutoVisible
      };

      const blockSize = this.nsForm.getValue(NsFormField.NAMESPACE_SIZE);
      if (blockSize) {
        request.block_size = blockSize;
      }

      if (isGatewayProvisioned) {
        request.rbd_image_name = `nvme_${pool}_${this.group}_${this.randomString()}`;
        if (rbdImageSize) {
          request['rbd_image_size'] = rbdImageSize;
        }
      }

      const rbdImageName = this.nsForm.getValue(NsFormField.RBD_IMAGE_NAME);
      if (rbdImageName) {
        request['rbd_image_name'] = loopCount > 1 ? `${rbdImageName}-${i}` : rbdImageName;
      }

      const subsystemNQN = this.nsForm.getValue(NsFormField.SUBSYSTEM) || this.subsystemNQN;
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
    const taskUrl: string = `nvmeof/namespace/${this.edit ? URLVerbs.EDIT : URLVerbs.CREATE}`;
    const image_size = this.nsForm.getValue(NsFormField.IMAGE_SIZE);
    const nsCount = this.nsForm.getValue(NsFormField.NS_COUNT);
    const hostAccess = this.nsForm.getValue(NsFormField.HOST_ACCESS);
    const selectedHosts: string[] = this.nsForm.getValue(NsFormField.INITIATORS) || [];
    const noAutoVisible = hostAccess === HOST_TYPE.SPECIFIC;
    let action: Observable<any>;
    let rbdImageSize: number = null;

    if (image_size) {
      rbdImageSize = this.formatterService.toBytes(image_size);
    }

    if (this.edit) {
      action = this.taskWrapperService.wrapTaskAroundCall({
        task: new FinishedTask(taskUrl, {
          nqn: this.subsystemNQN,
          nsid: this.nsid
        }),
        call: this.buildUpdateRequest(rbdImageSize)
      });
    } else {
      const subsystemNQN = this.nsForm.getValue(NsFormField.SUBSYSTEM);

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
    }

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

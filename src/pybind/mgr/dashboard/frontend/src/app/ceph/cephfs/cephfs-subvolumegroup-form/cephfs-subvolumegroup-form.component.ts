import { Component, OnInit } from '@angular/core';
import { FormControl, Validators } from '@angular/forms';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { CephfsSubvolumeGroupService } from '~/app/shared/api/cephfs-subvolume-group.service';
import { ActionLabelsI18n, URLVerbs } from '~/app/shared/constants/app.constants';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { Pool } from '../../pool/pool';
import { FormatterService } from '~/app/shared/services/formatter.service';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import _ from 'lodash';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { CdForm } from '~/app/shared/forms/cd-form';
import { DimlessBinaryPipe } from '~/app/shared/pipes/dimless-binary.pipe';
import { OctalToHumanReadablePipe } from '~/app/shared/pipes/octal-to-human-readable.pipe';

@Component({
  selector: 'cd-cephfs-subvolumegroup-form',
  templateUrl: './cephfs-subvolumegroup-form.component.html',
  styleUrls: ['./cephfs-subvolumegroup-form.component.scss']
})
export class CephfsSubvolumegroupFormComponent extends CdForm implements OnInit {
  fsName: string;
  subvolumegroupName: string;
  pools: Pool[];
  isEdit: boolean = false;

  subvolumegroupForm: CdFormGroup;

  action: string;
  resource: string;

  dataPools: Pool[];

  columns: CdTableColumn[];
  scopePermissions: Array<any> = [];
  initialMode = {
    owner: ['read', 'write', 'execute'],
    group: ['read', 'execute'],
    others: ['read', 'execute']
  };
  scopes: string[] = ['owner', 'group', 'others'];

  constructor(
    public activeModal: NgbActiveModal,
    private actionLabels: ActionLabelsI18n,
    private taskWrapper: TaskWrapperService,
    private cephfsSubvolumeGroupService: CephfsSubvolumeGroupService,
    private formatter: FormatterService,
    private dimlessBinary: DimlessBinaryPipe,
    private octalToHumanReadable: OctalToHumanReadablePipe
  ) {
    super();
    this.resource = $localize`subvolume group`;
  }

  ngOnInit(): void {
    this.action = this.actionLabels.CREATE;
    this.columns = [
      {
        prop: 'scope',
        name: $localize`All`,
        flexGrow: 0.5
      },
      {
        prop: 'read',
        name: $localize`Read`,
        flexGrow: 0.5,
        cellClass: 'text-center'
      },
      {
        prop: 'write',
        name: $localize`Write`,
        flexGrow: 0.5,
        cellClass: 'text-center'
      },
      {
        prop: 'execute',
        name: $localize`Execute`,
        flexGrow: 0.5,
        cellClass: 'text-center'
      }
    ];

    this.dataPools = this.pools.filter((pool) => pool.type === 'data');
    this.createForm();

    this.isEdit ? this.populateForm() : this.loadingReady();
  }

  createForm() {
    this.subvolumegroupForm = new CdFormGroup({
      volumeName: new FormControl({ value: this.fsName, disabled: true }),
      subvolumegroupName: new FormControl('', {
        validators: [Validators.required, Validators.pattern(/^[.A-Za-z0-9_-]+$/)],
        asyncValidators: [
          CdValidators.unique(
            this.cephfsSubvolumeGroupService.exists,
            this.cephfsSubvolumeGroupService,
            null,
            null,
            this.fsName
          )
        ]
      }),
      pool: new FormControl(this.dataPools[0]?.pool, {
        validators: [Validators.required]
      }),
      size: new FormControl(null, {
        updateOn: 'blur'
      }),
      uid: new FormControl(null),
      gid: new FormControl(null),
      mode: new FormControl({})
    });
  }

  populateForm() {
    this.action = this.actionLabels.EDIT;
    this.cephfsSubvolumeGroupService
      .info(this.fsName, this.subvolumegroupName)
      .subscribe((resp: any) => {
        // Disabled these fields since its not editable
        this.subvolumegroupForm.get('subvolumegroupName').disable();

        this.subvolumegroupForm.get('subvolumegroupName').setValue(this.subvolumegroupName);
        if (resp.bytes_quota !== 'infinite') {
          this.subvolumegroupForm
            .get('size')
            .setValue(this.dimlessBinary.transform(resp.bytes_quota));
        }
        this.subvolumegroupForm.get('uid').setValue(resp.uid);
        this.subvolumegroupForm.get('gid').setValue(resp.gid);
        this.initialMode = this.octalToHumanReadable.transform(resp.mode, true);

        this.loadingReady();
      });
  }

  submit() {
    const subvolumegroupName = this.subvolumegroupForm.getValue('subvolumegroupName');
    const pool = this.subvolumegroupForm.getValue('pool');
    const size = this.formatter.toBytes(this.subvolumegroupForm.getValue('size')) || 0;
    const uid = this.subvolumegroupForm.getValue('uid');
    const gid = this.subvolumegroupForm.getValue('gid');
    const mode = this.formatter.toOctalPermission(this.subvolumegroupForm.getValue('mode'));
    if (this.isEdit) {
      this.taskWrapper
        .wrapTaskAroundCall({
          task: new FinishedTask('cephfs/subvolume/group/' + URLVerbs.EDIT, {
            subvolumegroupName: subvolumegroupName
          }),
          call: this.cephfsSubvolumeGroupService.create(
            this.fsName,
            subvolumegroupName,
            pool,
            String(size),
            uid,
            gid,
            mode
          )
        })
        .subscribe({
          error: () => {
            this.subvolumegroupForm.setErrors({ cdSubmitButton: true });
          },
          complete: () => {
            this.activeModal.close();
          }
        });
    } else {
      this.taskWrapper
        .wrapTaskAroundCall({
          task: new FinishedTask('cephfs/subvolume/group/' + URLVerbs.CREATE, {
            subvolumegroupName: subvolumegroupName
          }),
          call: this.cephfsSubvolumeGroupService.create(
            this.fsName,
            subvolumegroupName,
            pool,
            String(size),
            uid,
            gid,
            mode
          )
        })
        .subscribe({
          error: () => {
            this.subvolumegroupForm.setErrors({ cdSubmitButton: true });
          },
          complete: () => {
            this.activeModal.close();
          }
        });
    }
  }
}

import { Component, OnInit } from '@angular/core';
import { FormControl, Validators } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';
import { CdForm } from '~/app/shared/forms/cd-form';
import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';

import _ from 'lodash';
import { of } from 'rxjs';
import { catchError, map } from 'rxjs/operators';

import { ActionLabelsI18n, URLVerbs } from '~/app/shared/constants/app.constants';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { ComboBoxItem } from '~/app/shared/models/combo-box.model';

import { Filesystem, PROVIDER, SHARE_RESOURCE, ShareRequestModel, SMBShare } from '../smb.model';
import { CephfsSubvolumeGroup } from '~/app/shared/models/cephfs-subvolume-group.model';
import { CephfsSubvolume } from '~/app/shared/models/cephfs-subvolume.model';

import { SmbService } from '~/app/shared/api/smb.service';
import { NfsService } from '~/app/shared/api/nfs.service';
import { RgwBucketService } from '~/app/shared/api/rgw-bucket.service';
import { RgwUserService } from '~/app/shared/api/rgw-user.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { FormatterService } from '~/app/shared/services/formatter.service';
import { DimlessBinaryPipe } from '~/app/shared/pipes/dimless-binary.pipe';
import { CephfsSubvolumeGroupService } from '~/app/shared/api/cephfs-subvolume-group.service';
import { CephfsSubvolumeService } from '~/app/shared/api/cephfs-subvolume.service';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { resolveSmbRouteData } from '../smb-route.util';

const QOS_IOPS_MAX = 1_000_000;
const QOS_BW_MAX_BYTES = 2 ** 40;
const UNIT_TO_BYTES: Record<string, number> = {
  KiB: 2 ** 10,
  MiB: 2 ** 20,
  GiB: 2 ** 30,
  TiB: 2 ** 40
};
const QOS_BURST_MULT_MIN = 10;
const QOS_BURST_MULT_MAX = 100;
const QOS_BURST_MULT_DEFAULT = 15;
const QOS_BW_UNITS = ['KiB', 'MiB', 'GiB', 'TiB'];

function getBwMaxForUnit(unit: string): number {
  return Math.floor(QOS_BW_MAX_BYTES / UNIT_TO_BYTES[unit]);
}

@Component({
  selector: 'cd-smb-share-form',
  templateUrl: './smb-share-form.component.html',
  styleUrls: ['./smb-share-form.component.scss'],
  standalone: false
})
export class SmbShareFormComponent extends CdForm implements OnInit {
  smbShareForm: CdFormGroup;
  action: string;
  resource: string;
  allFsNames: Filesystem[] = [];
  allsubvolgrps: CephfsSubvolumeGroup[] = [];
  allsubvols: CephfsSubvolume[] = [];
  clusterId: string;
  isEdit = false;
  share_id: string;
  shareResponse: SMBShare;
  qosBwUnits = QOS_BW_UNITS;
  readBwMax = getBwMaxForUnit(QOS_BW_UNITS[1]);
  writeBwMax = getBwMaxForUnit(QOS_BW_UNITS[1]);
  isRgw = false;
  bucketItems: ComboBoxItem[] = [];
  allRgwUsers: string[] | null = null;
  private loadedBucketNames = new Set<string>();
  private smbBasePath: string;

  constructor(
    private formBuilder: CdFormBuilder,
    public smbService: SmbService,
    public actionLabels: ActionLabelsI18n,
    private nfsService: NfsService,
    private subvolgrpService: CephfsSubvolumeGroupService,
    private subvolService: CephfsSubvolumeService,
    private taskWrapperService: TaskWrapperService,
    private router: Router,
    private route: ActivatedRoute,
    private formatter: FormatterService,
    private dimlessBinaryPipe: DimlessBinaryPipe,
    private rgwBucketService: RgwBucketService,
    private rgwUserService: RgwUserService
  ) {
    super();
    this.resource = $localize`Share`;
    const smbRoute = resolveSmbRouteData(this.route);
    this.isRgw = smbRoute.isRgw;
    this.smbBasePath = smbRoute.smbBasePath;
    this.isEdit = !!this.route.snapshot.data['editing'];
    this.action = this.isEdit ? this.actionLabels.EDIT : this.actionLabels.CREATE;
  }
  ngOnInit() {
    this.route.params.subscribe((params: any) => {
      this.share_id = params.shareId;
      this.clusterId = params.clusterId;
    });
    this.createForm();
    if (!this.isRgw) {
      this.nfsService.filesystems().subscribe((data: Filesystem[]) => {
        this.allFsNames = data;
      });
    }
    if (this.isRgw) {
      this.loadBucketItems();
      this.loadRgwUsers();
    }
    if (this.isEdit) {
      this.smbService.getShare(this.clusterId, this.share_id).subscribe((resp: SMBShare) => {
        this.shareResponse = resp;
        if (this.isRgw) {
          const bucket = this.shareResponse.rgw?.bucket;
          this.ensureBucketItem(bucket);
          const bucketCtrl = this.smbShareForm.get('bucket');
          // Mark dirty before setting value so bucketExistence does not treat
          // a filled control as still pristine/required.
          bucketCtrl.markAsDirty();
          this.smbShareForm.patchValue({
            share_id: this.shareResponse.share_id,
            name: this.shareResponse.name,
            bucket,
            user_id: this.shareResponse.rgw?.user_id || '',
            readonly: this.shareResponse.readonly ?? false,
            browseable: this.shareResponse.browseable ?? true
          });
          this.smbShareForm.get('share_id').disable();
          this.smbShareForm.get('name').disable();
        } else {
          const cephfs = this.shareResponse?.cephfs;
          const qos = cephfs?.qos;

          this.smbShareForm.patchValue({
            share_id: this.shareResponse.share_id,
            name: this.shareResponse.name,
            volume: cephfs?.volume,
            subvolume_group: cephfs?.subvolumegroup,
            subvolume: cephfs?.subvolume,
            inputPath: cephfs?.path,
            readonly: this.shareResponse.readonly ?? false,
            browseable: this.shareResponse.browseable ?? true,
            read_iops_limit: qos?.read_iops_limit,
            write_iops_limit: qos?.write_iops_limit,
            read_burst_mult: qos?.read_burst_mult,
            write_burst_mult: qos?.write_burst_mult
          });
          this.smbShareForm.get('share_id').disable();
          this.smbShareForm.get('name').disable();
          this.setBwLimitFromBytes('read_bw_limit', qos?.read_bw_limit);
          this.setBwLimitFromBytes('write_bw_limit', qos?.write_bw_limit);

          this.getSubVolGrp(cephfs?.volume);
        }
      });
    }
    this.smbShareForm.get('read_bw_limit_unit').valueChanges.subscribe((unit: string) => {
      this.readBwMax = getBwMaxForUnit(unit);
      const ctrl = this.smbShareForm.get('read_bw_limit');
      ctrl.setValidators([Validators.min(0), Validators.max(this.readBwMax)]);
      ctrl.updateValueAndValidity();
    });
    this.smbShareForm.get('write_bw_limit_unit').valueChanges.subscribe((unit: string) => {
      this.writeBwMax = getBwMaxForUnit(unit);
      const ctrl = this.smbShareForm.get('write_bw_limit');
      ctrl.setValidators([Validators.min(0), Validators.max(this.writeBwMax)]);
      ctrl.updateValueAndValidity();
    });
    this.smbShareForm.get('share_id')?.valueChanges.subscribe((value) => {
      const shareName = this.smbShareForm.get('name');
      if (shareName && !shareName.dirty) {
        shareName.setValue(value, { emitEvent: false });
      }
    });
    this.loadingReady();
  }

  private setBwLimitFromBytes(prefix: string, bytes: number) {
    const parts = this.dimlessBinaryPipe.transform(bytes).split(' ');
    this.smbShareForm.get(prefix).setValue(parts[0] || 0);
    this.smbShareForm.get(prefix + '_unit').setValue(parts[1] || QOS_BW_UNITS[1]);
  }

  createForm() {
    this.smbShareForm = this.formBuilder.group({
      share_id: new FormControl('', {
        validators: [Validators.required]
      }),
      name: new FormControl(''),
      volume: new FormControl('', {
        validators: this.isRgw ? [] : [Validators.required]
      }),
      subvolume_group: new FormControl(''),
      subvolume: new FormControl(''),
      prefixedPath: new FormControl({ value: '', disabled: true }),
      inputPath: new FormControl('/', {
        validators: this.isRgw ? [] : [Validators.required]
      }),
      bucket: new FormControl('', {
        validators: this.isRgw ? [Validators.required] : [],
        asyncValidators: this.isRgw
          ? [CdValidators.bucketExistence(true, this.rgwBucketService)]
          : []
      }),
      user_id: new FormControl(''),
      browseable: new FormControl(true),
      readonly: new FormControl(false),
      read_iops_limit: new FormControl(0, [Validators.min(0), Validators.max(QOS_IOPS_MAX)]),
      write_iops_limit: new FormControl(0, [Validators.min(0), Validators.max(QOS_IOPS_MAX)]),
      read_bw_limit: new FormControl(0, [Validators.min(0), Validators.max(this.readBwMax)]),
      read_bw_limit_unit: new FormControl(QOS_BW_UNITS[1]),
      write_bw_limit: new FormControl(0, [Validators.min(0), Validators.max(this.writeBwMax)]),
      write_bw_limit_unit: new FormControl(QOS_BW_UNITS[1]),
      read_burst_mult: new FormControl(QOS_BURST_MULT_DEFAULT, [
        Validators.min(QOS_BURST_MULT_MIN),
        Validators.max(QOS_BURST_MULT_MAX)
      ]),
      write_burst_mult: new FormControl(QOS_BURST_MULT_DEFAULT, [
        Validators.min(QOS_BURST_MULT_MIN),
        Validators.max(QOS_BURST_MULT_MAX)
      ])
    });

    if (this.isRgw) {
      // Combo box may emit ListItem objects; keep the control value as a string
      // so existence checks hit /api/rgw/bucket/<name> correctly.
      this.smbShareForm.get('bucket').valueChanges.subscribe((value) => {
        const normalized = this.normalizeBucketName(value);
        if (value !== normalized) {
          this.smbShareForm.get('bucket').setValue(normalized, { emitEvent: false });
        }
        if (normalized) {
          this.suggestBucketOwner(normalized);
        }
      });
    }
  }

  volumeChangeHandler() {
    const fsName = this.smbShareForm.getValue('volume');
    this.smbShareForm.patchValue({
      subvolume_group: '',
      subvolume: '',
      prefixedPath: ''
    });
    this.allsubvols = [];
    if (fsName) {
      this.getSubVolGrp(fsName);
    }
  }

  getSubVolGrp(volume: string) {
    this.smbShareForm.patchValue({
      subvolume_group: '',
      subvolume: ''
    });
    if (volume) {
      this.subvolgrpService.get(volume).subscribe((data: CephfsSubvolumeGroup[]) => {
        this.allsubvolgrps = data;
        if (this.isEdit) {
          const selectedSubVolGrp = this.shareResponse.cephfs.subvolumegroup;
          if (selectedSubVolGrp && volume === this.shareResponse.cephfs.volume) {
            const subvolGrp = this.allsubvolgrps.find((group) => group.name === selectedSubVolGrp);
            if (subvolGrp) {
              this.smbShareForm.get('subvolume_group').setValue(subvolGrp.name);
              this.getSubVol();
            }
          }
        }
      });
    }
  }

  async getSubVol() {
    const volume = this.smbShareForm.getValue('volume');
    const subvolgrp = this.smbShareForm.getValue('subvolume_group');
    this.smbShareForm.patchValue({
      subvolume: '',
      prefixedPath: ''
    });
    this.allsubvols = [];

    if (volume && subvolgrp) {
      await this.setSubVolPath();
      this.subvolService.get(volume, subvolgrp, false).subscribe((data: CephfsSubvolume[]) => {
        this.allsubvols = data;
        if (this.isEdit) {
          const selectedSubVol = this.shareResponse.cephfs.subvolume;
          if (selectedSubVol && this.shareResponse.cephfs.subvolumegroup) {
            const subvol = this.allsubvols.find((s) => s.name === selectedSubVol);
            if (subvol) {
              this.smbShareForm.get('subvolume').setValue(subvol.name);
              this.setSubVolPath();
            }
          }
        }
      });
    }
  }

  setSubVolPath(): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      const fsName = this.smbShareForm.getValue('volume');
      const subvolGroup = this.smbShareForm.getValue('subvolume_group') || ''; // Default to empty if not present
      const subvol = this.smbShareForm.getValue('subvolume');

      if (subvol) {
        this.subvolService
          .info(fsName, subvol, subvolGroup)
          .pipe(map((data: any) => data['path']))
          .subscribe(
            (path: string) => {
              this.updatePath(path);
              resolve();
            },
            (error: any) => reject(error)
          );
      } else {
        this.updatePath(`/volumes/${subvolGroup}/`);
        resolve();
      }
    });
  }

  updatePath(prefixedPath: string) {
    this.smbShareForm.patchValue({ prefixedPath: prefixedPath });
  }

  private loadBucketItems(): void {
    this.rgwBucketService
      .list()
      .pipe(
        map((bucketNames: string[]) =>
          bucketNames.map((name) => ({
            content: name,
            name,
            selected: false
          }))
        ),
        catchError(() => of([]))
      )
      .subscribe((items) => {
        this.bucketItems = items;
        this.loadedBucketNames = new Set(items.map((item) => item.content));
      });
  }

  private loadRgwUsers(): void {
    this.rgwUserService
      .enumerate()
      .pipe(catchError(() => of([])))
      .subscribe((uids: string[]) => {
        this.allRgwUsers = uids;
      });
  }

  /**
   * Prefill user_id with the bucket owner when the field is still empty and the
   * owner exists in the RGW user list. Leave empty for automatic resolution.
   * Only runs for buckets known from the list API — never GET a typed name that
   * may not exist (that would surface a 404 toast).
   */
  private suggestBucketOwner(bucket: string): void {
    const userCtrl = this.smbShareForm.get('user_id');
    if (!userCtrl || userCtrl.value || userCtrl.dirty) {
      return;
    }
    if (!this.loadedBucketNames.has(bucket)) {
      return;
    }
    this.rgwBucketService
      .get(bucket)
      .pipe(
        catchError((error) => {
          if (_.isFunction(error?.preventDefault)) {
            error.preventDefault();
          }
          return of(null);
        })
      )
      .subscribe((details: { owner?: string } | null) => {
        const owner = details?.owner;
        if (!owner || userCtrl.value || userCtrl.dirty) {
          return;
        }
        if (this.allRgwUsers?.includes(owner)) {
          userCtrl.setValue(owner, { emitEvent: false });
        }
      });
  }

  private ensureBucketItem(bucket?: string): void {
    if (!bucket || this.bucketItems.some((item) => item.content === bucket)) {
      return;
    }
    this.bucketItems = [
      ...this.bucketItems,
      { content: bucket, name: bucket, selected: false }
    ];
  }

  /**
   * Carbon single-select combo box emits submit on Enter so a typed name can
   * be added to the list and selected.
   */
  onBucketSubmit(event: {
    value?: { content?: string };
  }): void {
    const content = event?.value?.content?.trim();
    if (!content) {
      return;
    }
    this.ensureBucketItem(content);
    const bucketCtrl = this.smbShareForm.get('bucket');
    bucketCtrl.markAsDirty();
    bucketCtrl.setValue(content);
  }

  private normalizeBucketName(bucket: any): string {
    if (_.isString(bucket)) {
      return bucket;
    }
    return bucket?.content || '';
  }

  buildRequest() {
    const rawFormValue = _.cloneDeep(this.smbShareForm.value);
    const shareId = this.smbShareForm.get('share_id')?.value;
    const shareName = this.smbShareForm.get('name').value;
    const requestModel: ShareRequestModel = {
      share_resource: {
        resource_type: SHARE_RESOURCE,
        cluster_id: this.clusterId,
        share_id: shareId,
        name: shareName,
        browseable: rawFormValue.browseable,
        readonly: rawFormValue.readonly
      }
    };
    if (this.isRgw) {
      requestModel.share_resource.rgw = {
        bucket: this.normalizeBucketName(rawFormValue.bucket)
      };
      const userId = (rawFormValue.user_id || '').trim();
      if (userId) {
        requestModel.share_resource.rgw.user_id = userId;
      }
    } else {
      requestModel.share_resource.cephfs = {
        volume: rawFormValue.volume,
        path: rawFormValue.inputPath,
        subvolumegroup: rawFormValue.subvolume_group,
        subvolume: rawFormValue.subvolume,
        provider: PROVIDER,
        qos: {
          read_iops_limit: rawFormValue.read_iops_limit,
          write_iops_limit: rawFormValue.write_iops_limit,
          read_bw_limit: this.formatter.toBytes(
            String(this.smbShareForm.get('read_bw_limit').value) +
              ' ' +
              this.smbShareForm.get('read_bw_limit_unit').value
          ),
          write_bw_limit: this.formatter.toBytes(
            String(this.smbShareForm.get('write_bw_limit').value) +
              ' ' +
              this.smbShareForm.get('write_bw_limit_unit').value
          ),
          read_burst_mult: rawFormValue.read_burst_mult,
          write_burst_mult: rawFormValue.write_burst_mult
        }
      };
    }
    return requestModel;
  }

  submitAction() {
    // Block implicit submits (e.g. Enter in the bucket combo box) while the
    // form is incomplete or async bucket checks are still pending.
    if (this.smbShareForm.invalid || this.smbShareForm.pending) {
      this.smbShareForm.markAllAsTouched();
      return;
    }
    if (this.isEdit) {
      this.handleTaskRequest(URLVerbs.EDIT);
    } else {
      this.handleTaskRequest(URLVerbs.CREATE);
    }
  }

  handleTaskRequest(urlVerb: string) {
    const requestModel = this.buildRequest();
    const component = this;
    const share_id = this.smbShareForm.get('share_id').value;

    this.taskWrapperService
      .wrapTaskAroundCall({
        task: new FinishedTask(`${this.smbBasePath}/share/${urlVerb}`, { share_id }),
        call: this.smbService.createShare(requestModel)
      })
      .subscribe({
        complete: () => {
          this.router.navigate([`${this.smbBasePath}/cluster`]);
        },
        error: () => {
          component.smbShareForm.setErrors({ cdSubmitButton: true });
        }
      });
  }
}

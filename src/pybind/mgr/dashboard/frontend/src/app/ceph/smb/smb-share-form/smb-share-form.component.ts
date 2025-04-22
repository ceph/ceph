import { Component, OnInit } from '@angular/core';
import { FormControl, Validators } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';
import { CdForm } from '~/app/shared/forms/cd-form';
import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';

import _ from 'lodash';
import { map } from 'rxjs/operators';
import { Observable } from 'rxjs';

import { ActionLabelsI18n, URLVerbs } from '~/app/shared/constants/app.constants';
import { FinishedTask } from '~/app/shared/models/finished-task';

import {
  Filesystem,
  PROVIDER,
  SHARE_RESOURCE,
  SHARE_URL,
  ShareRequestModel,
  SMBCluster,
  SMBShare
} from '../smb.model';
import { CephfsSubvolumeGroup } from '~/app/shared/models/cephfs-subvolume-group.model';
import { CephfsSubvolume } from '~/app/shared/models/cephfs-subvolume.model';

import { SmbService } from '~/app/shared/api/smb.service';
import { NfsService } from '~/app/shared/api/nfs.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { CephfsSubvolumeGroupService } from '~/app/shared/api/cephfs-subvolume-group.service';
import { CephfsSubvolumeService } from '~/app/shared/api/cephfs-subvolume.service';

@Component({
  selector: 'cd-smb-share-form',
  templateUrl: './smb-share-form.component.html',
  styleUrls: ['./smb-share-form.component.scss']
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
  smbClusters$: Observable<SMBCluster[]>;
  selectedFsName: string;
  selectedSubvolGroup: string;
  selectedSubvol: string;
  cluster_id: string;

  constructor(
    private formBuilder: CdFormBuilder,
    public smbService: SmbService,
    public actionLabels: ActionLabelsI18n,
    private nfsService: NfsService,
    private subvolgrpService: CephfsSubvolumeGroupService,
    private subvolService: CephfsSubvolumeService,
    private taskWrapperService: TaskWrapperService,
    private router: Router,
    private route: ActivatedRoute
  ) {
    super();
    this.resource = $localize`Share`;
    this.isEdit = this.router.url.startsWith(`${SHARE_URL}${URLVerbs.EDIT}`);
    this.action = this.isEdit ? this.actionLabels.EDIT : this.actionLabels.CREATE;
  }
  ngOnInit() {
    this.route.params.subscribe((params: any) => {
      this.share_id = params.shareId;
      this.clusterId = params.clusterId;
    });
    this.smbClusters$ = this.smbService.listClusters();
    this.createForm();
    if (this.isEdit) {
      this.smbService.getShare(this.clusterId, this.share_id).subscribe((resp: SMBShare) => {
        this.shareResponse = resp;
        this.smbShareForm.get('share_id').setValue(this.shareResponse.share_id);
        this.smbShareForm.get('share_id').disable();
        this.smbShareForm.get('volume').setValue(this.shareResponse.cephfs.volume);
        this.smbShareForm.get('subvolume_group').setValue(this.shareResponse.cephfs.subvolumegroup);
        this.smbShareForm.get('subvolume').setValue(this.shareResponse.cephfs.subvolume);
        this.smbShareForm.get('inputPath').setValue(this.shareResponse.cephfs.path);
        if (this.shareResponse.readonly) {
          this.smbShareForm.get('readonly').setValue(this.shareResponse.readonly);
        }
        if (this.shareResponse.browseable) {
          this.smbShareForm.get('browseable').setValue(this.shareResponse.browseable);
        }

        this.getSubVolGrp(this.shareResponse.cephfs.volume);
      });
    }

    this.route.params.subscribe(
      (params: { fs_name: string; subvolume_group: string; subvolume?: string }) => {
        this.selectedFsName = params.fs_name;
        this.selectedSubvolGroup = params.subvolume_group;
        if (params.subvolume) this.selectedSubvol = params.subvolume;
        this.getData(this.selectedFsName);
      }
    );
    if (!this.isEdit) {
      this.resolveRouteParams();
    }
    this.loadingReady();
  }

  createForm() {
    this.smbShareForm = this.formBuilder.group({
      share_id: new FormControl('', {
        validators: [Validators.required]
      }),
      volume: new FormControl('', {
        validators: [Validators.required]
      }),
      subvolume_group: new FormControl(''),
      subvolume: new FormControl(''),
      prefixedPath: new FormControl({ value: '', disabled: true }),
      inputPath: new FormControl('/', {
        validators: [Validators.required]
      }),
      browseable: new FormControl(true),
      readonly: new FormControl(false),
      cluster_id: new FormControl('', {
        validators: [Validators.required]
      })
    });
  }

  getData(fsName: string) {
    this.nfsService.filesystems().subscribe((data: Filesystem[]) => {
      this.allFsNames = data;
      const vol = this.allFsNames.find((vol) => vol.name === fsName);
      if (vol) {
        this.smbShareForm.get('volume').setValue(vol.name);
      }
    });
  }

  resolveFilesystems(filesystems: any[]) {
    this.allFsNames = filesystems;
  }

  resolveRouteParams() {
    if (!_.isEmpty(this.selectedFsName)) {
      this.smbShareForm.patchValue({
        volume: this.selectedFsName
      });
      this.getSubVolGrp(this.selectedFsName);
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
        const subvolgrp = this.allsubvolgrps.find((grp) => grp.name === this.selectedSubvolGroup);
        if (subvolgrp) {
          this.smbShareForm.get('subvolume_group').setValue(subvolgrp.name);
          this.getSubVol();
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
        const subvol = this.allsubvols.find((vol) => vol.name === this.selectedSubvol);
        if (subvolgrp && subvol) {
          this.smbShareForm.get('subvolume').setValue(subvol.name);
          this.setSubVolPath();
        }
      });
    }
  }

  setSubVolPath(): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      const fsName = this.smbShareForm.getValue('volume');
      const subvolGroup = this.smbShareForm.getValue('subvolume_group') || ''; // Default to empty if not present
      const subvol = this.smbShareForm.getValue('subvolume');

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
    });
  }

  updatePath(prefixedPath: string) {
    this.smbShareForm.patchValue({ prefixedPath: prefixedPath });
  }

  buildRequest() {
    const rawFormValue = _.cloneDeep(this.smbShareForm.value);
    const correctedPath = rawFormValue.inputPath;
    const shareId = this.smbShareForm.get('share_id')?.value;

    if (this.clusterId) {
      this.cluster_id = this.smbShareForm.get('cluster_id').value;
    } else {
      this.cluster_id = this.smbShareForm.get('cluster_id').value;
    }
    const requestModel: ShareRequestModel = {
      share_resource: {
        resource_type: SHARE_RESOURCE,
        cluster_id: this.cluster_id,
        share_id: shareId,
        cephfs: {
          volume: rawFormValue.volume,
          path: correctedPath,
          subvolumegroup: rawFormValue.subvolume_group,
          subvolume: rawFormValue.subvolume,
          provider: PROVIDER
        },
        browseable: rawFormValue.browseable,
        readonly: rawFormValue.readonly
      }
    };

    return requestModel;
  }

  submitAction() {
    if (this.isEdit) {
      this.handleTaskRequest(URLVerbs.EDIT);
    } else {
      this.handleTaskRequest(URLVerbs.CREATE);
    }
  }

  goToCreateCluster() {
    this.router.navigate(['cephfs/smb/clusters/create']);
  }

  handleTaskRequest(urlVerb: string) {
    const requestModel = this.buildRequest();
    const BASE_URL = 'smb/share';
    const component = this;
    const share_id = this.smbShareForm.get('share_id').value;

    this.taskWrapperService
      .wrapTaskAroundCall({
        task: new FinishedTask(`${BASE_URL}/${urlVerb}`, { share_id }),
        call: this.smbService.createShare(requestModel)
      })
      .subscribe({
        complete: () => {
          this.router.navigate([`cephfs/smb`]);
        },
        error: () => {
          component.smbShareForm.setErrors({ cdSubmitButton: true });
        }
      });
  }
}

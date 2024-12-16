import { Component, OnInit } from '@angular/core';
import { AbstractControl } from '@angular/forms';
import { ValidationErrors } from '@angular/forms';
import { AsyncValidatorFn } from '@angular/forms';
import { FormControl, Validators } from '@angular/forms';
import { CdForm } from '~/app/shared/forms/cd-form';
import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { ActivatedRoute } from '@angular/router';
import { Observable, of } from 'rxjs';

import _ from 'lodash';
import { catchError, debounceTime, distinctUntilChanged, map, mergeMap } from 'rxjs/operators';

import { ActionLabelsI18n, URLVerbs } from '~/app/shared/constants/app.constants';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { Router } from '@angular/router';

import { PROVIDER, RESOURCE_TYPE } from '../smb.model';
import { DEFAULT_SUBVOLUME_GROUP } from '~/app/shared/constants/cephfs.constant';

import { CephfsSubvolumeGroupService } from '~/app/shared/api/cephfs-subvolume-group.service';
import { CephfsSubvolumeService } from '~/app/shared/api/cephfs-subvolume.service';
import { SmbService } from '~/app/shared/api/smb.service';
import { Directory, NfsService } from '~/app/shared/api/nfs.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';

@Component({
  selector: 'cd-smb-share-form',
  templateUrl: './smb-share-form.component.html',
  styleUrls: ['./smb-share-form.component.scss']
})
export class SmbShareFormComponent extends CdForm implements OnInit {
  smbShareForm: CdFormGroup;
  action: string;
  resource: string;
  allFsNames: any;
  allsubvolgrps: any[] = [];
  allsubvols: any[] = [];
  defaultSubVolGroup = DEFAULT_SUBVOLUME_GROUP;
  clusterId: string;

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
  ) {
    super();
    this.resource = $localize`Share`;
  }
  ngOnInit() {
    this.action = this.actionLabels.CREATE;
    this.route.params.subscribe(
          (params: {clusterId: string}) => {
            this.clusterId = params.clusterId; 
          }
    );
    this.createForm();
    //  this.setPathValidation();
    this.nfsService.filesystems().subscribe((data: any) => {
      this.allFsNames = data;
    });
  }

  createForm() {
    this.smbShareForm = this.formBuilder.group({
      share_id: new FormControl('', {
        validators: [Validators.required]
      }),
      volume: new FormControl('', {
        validators: [Validators.required]
      }),
      subvolume_group: new FormControl(this.defaultSubVolGroup),
      subvolume: new FormControl(''),
      path: new FormControl('', {
        validators: [
          Validators.required,
          CdValidators.custom('isIsolatedSlash', this.isolatedSlashCondition) // Path can never be single "/".
        ]
      }),
      browseable: new FormControl(true),
      readonly:  new FormControl(false),
    });
  }

  volumeChangeHandler() {
    const fsName = this.smbShareForm.getValue('volume');
    if (fsName) {
      this.isDefaultSubvolumeGroup();
    }
    this.updatePath('');
  }

  // Checking if subVolGroup is "_nogroup" and updating path to default as "/volumes/_nogroup else blank."
  isDefaultSubvolumeGroup() {
    const fsName = this.smbShareForm.getValue('volume');
    this.getSubVolGrp(fsName);
    this.getSubVol();
    this.updatePath('/volumes/' + this.defaultSubVolGroup);
    this.setUpVolumeValidation();
  }

  setUpVolumeValidation() {
    const subvolumeGroup = this.smbShareForm.get('subvolume_group').value;
    const subVolumeControl = this.smbShareForm.get('subvolume');

    // SubVolume is required if SubVolume Group is "_nogroup".
    if (subvolumeGroup == this.defaultSubVolGroup) {
      subVolumeControl?.setValidators([Validators.required]);
    } else {
      subVolumeControl?.clearValidators();
    }
    subVolumeControl?.updateValueAndValidity();
  }

  updatePath(path: string) {
    this.smbShareForm.patchValue({ path: path });
  }

  getSubVolGrp(fs_name: string) {
    this.subvolgrpService.get(fs_name).subscribe((data: any) => {
      this.allsubvolgrps = data;
    });
  }

  async getSubVol() {
    const fs_name = this.smbShareForm.getValue('volume');
    const subvolgrp = this.smbShareForm.getValue('subvolume_group');
    await this.setSubVolGrpPath();

    (subvolgrp === this.defaultSubVolGroup
      ? this.subvolService.get(fs_name)
      : this.subvolService.get(fs_name, subvolgrp)
    ).subscribe((data: any) => {
      this.allsubvols = data;
    });
    this.setUpVolumeValidation();
  }

  setSubVolGrpPath(): Promise<void> {
    const fsName = this.smbShareForm.getValue('volume');
    const subvolGroup = this.smbShareForm.getValue('subvolume_group');

    return new Promise<void>((resolve, reject) => {
      if (subvolGroup == this.defaultSubVolGroup) {
        this.updatePath('/volumes/' + this.defaultSubVolGroup);
      } else if (subvolGroup != '') {
        this.subvolgrpService
          .info(fsName, subvolGroup)
          .pipe(map((data) => data['path']))
          .subscribe(
            (path) => {
              this.updatePath(path);
              resolve();
            },
            (error) => reject(error)
          );
      } else {
        this.updatePath('');
        this.setUpVolumeValidation();
      }
      resolve();
    });
  }

  setSubVolPath(){
     return new Promise<void>((resolve, reject) => {
      const subvol = this.smbShareForm.getValue('subvolume');
      const subvolGroup = this.smbShareForm.getValue('subvolume_group');
      const fs_name = this.smbShareForm.getValue('volume');

      const path = "/volumes/"+subvolGroup +"/" +subvol;
      this.updatePath(path);
      this.subvolService
        .info(fs_name, subvol, subvolGroup === this.defaultSubVolGroup ? '' : subvolGroup)
        .pipe(map((data) => data['path']))
        .subscribe(
          (path: string) => {
            this.updatePath(path);
            resolve();
          },
          (error) => reject(error)
        );
    });
  }

  isolatedSlashCondition(value: string): boolean {
    return value === '/';
  }

  pathDataSource = (text$: Observable<string>) => {
    return text$.pipe(
      debounceTime(200),
      distinctUntilChanged(),
      mergeMap((token: string) => this.getPathTypeahead(token)),
      map((val: string[]) => val)
    );
  };

  setPathValidation() {
    const path = this.smbShareForm.get('path');
    path.setAsyncValidators([this.pathExistence(true)]);
  }

  private pathExistence(requiredExistenceResult: boolean): AsyncValidatorFn {
    return (control: AbstractControl): Observable<ValidationErrors | null> => {
      if (control.pristine || !control.value) {
        return of({ required: true });
      }
      const fsName = this.smbShareForm.getValue('volume');
      return this.nfsService.lsDir(fsName, control.value).pipe(
        map((directory: Directory) =>
          directory.paths.includes(control.value) === requiredExistenceResult
            ? null
            : { pathNameNotAllowed: true }
        ),
        catchError(() => of({ pathNameNotAllowed: true }))
      );
    };
  }

  private getPathTypeahead(path: any) {
    if (!_.isString(path) || path === '/') {
      return of([]);
    }

    const fsName = this.smbShareForm.getValue('volume');
    return this.nfsService.lsDir(fsName, path).pipe(
      map((result: Directory) =>
        result.paths.filter((dirName: string) => dirName.toLowerCase().includes(path)).slice(0, 15)
      ),
      catchError(() => of([$localize`Error while retrieving paths.`]))
    );
  }

  private buildRequest() {
    const rawFormValue = _.cloneDeep(this.smbShareForm.value);

      const requestModel = {
      share_resource: {
        resource_type: RESOURCE_TYPE.share,
        cluster_id: this.clusterId,
        share_id: rawFormValue.share_id,
        cephfs: {
          volume: rawFormValue.volume,
          path: rawFormValue.path,
          subvolumegroup: rawFormValue.subvolume_group,
          subvolume: rawFormValue.subvolume,
          provider: PROVIDER
        },
        browseable:rawFormValue.browseable,
        readonly:rawFormValue.readonly
      }
    };

    return requestModel;
  }

  submitAction() {
    const component = this;
    const requestModel = this.buildRequest();
    const BASE_URL = 'smb/share';
    const share_id = this.smbShareForm.get('share_id').value;
    const taskUrl = `${BASE_URL}/${URLVerbs.CREATE}`;
    this.taskWrapperService
      .wrapTaskAroundCall({
        task: new FinishedTask(taskUrl, { share_id }),
        call: this.smbService.createShare(requestModel)
      })
      .subscribe({
        complete: () => {
          this.router.navigate([`cephfs/smb/share`]);
        },
        error() {
          component.smbShareForm.setErrors({ cdSubmitButton: true });
        }
      });
  }
}
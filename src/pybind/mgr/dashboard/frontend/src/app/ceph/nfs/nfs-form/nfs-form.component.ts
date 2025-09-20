import { Component, OnInit, ViewChild } from '@angular/core';
import {
  AbstractControl,
  AsyncValidatorFn,
  UntypedFormControl,
  ValidationErrors,
  Validators
} from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';

import _ from 'lodash';
import { forkJoin, Observable, of } from 'rxjs';
import { catchError, debounceTime, distinctUntilChanged, map, mergeMap } from 'rxjs/operators';

import { SUPPORTED_FSAL } from '~/app/ceph/nfs/models/nfs.fsal';
import { Directory, NfsService } from '~/app/shared/api/nfs.service';
import { RgwBucketService } from '~/app/shared/api/rgw-bucket.service';
import { RgwSiteService } from '~/app/shared/api/rgw-site.service';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { Icons } from '~/app/shared/enum/icons.enum';
import { CdForm } from '~/app/shared/forms/cd-form';
import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { Permission } from '~/app/shared/models/permissions';
import { CdHttpErrorResponse } from '~/app/shared/services/api-interceptor.service';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { NfsFormClientComponent } from '../nfs-form-client/nfs-form-client.component';
import { getFsalFromRoute, getPathfromFsal } from '../utils';
import { CephfsSubvolumeService } from '~/app/shared/api/cephfs-subvolume.service';
import { CephfsSubvolumeGroupService } from '~/app/shared/api/cephfs-subvolume-group.service';
import { RgwUserService } from '~/app/shared/api/rgw-user.service';
import { RgwExportType } from '../nfs-list/nfs-list.component';
import { DEFAULT_SUBVOLUME_GROUP } from '~/app/shared/constants/cephfs.constant';

@Component({
  selector: 'cd-nfs-form',
  templateUrl: './nfs-form.component.html',
  styleUrls: ['./nfs-form.component.scss']
})
export class NfsFormComponent extends CdForm implements OnInit {
  @ViewChild('nfsClients', { static: true })
  nfsClients: NfsFormClientComponent;

  clients: any[] = [];

  permission: Permission;
  nfsForm: CdFormGroup;
  isEdit = false;

  cluster_id: string = null;
  export_id: string = null;

  allClusters: { cluster_id: string }[] = null;
  icons = Icons;

  allFsNames: any[] = null;

  allRGWUsers: any[] = null;

  storageBackend: SUPPORTED_FSAL;
  storageBackendError: string = null;

  defaultAccessType = { RGW: 'RO' };
  nfsAccessType: any[] = [];
  nfsSquash: any[] = [];

  action: string;
  resource: string;

  allsubvolgrps: any[] = [];
  allsubvols: any[] = [];

  selectedFsName: string = '';
  selectedSubvolGroup: string = '';
  selectedSubvol: string = '';
  defaultSubVolGroup = DEFAULT_SUBVOLUME_GROUP;

  pathDataSource = (text$: Observable<string>) => {
    return text$.pipe(
      debounceTime(200),
      distinctUntilChanged(),
      mergeMap((token: string) => this.getPathTypeahead(token)),
      map((val: string[]) => val)
    );
  };

  bucketDataSource = (text$: Observable<string>) => {
    return text$.pipe(
      debounceTime(200),
      distinctUntilChanged(),
      mergeMap((token: string) => this.getBucketTypeahead(token))
    );
  };

  constructor(
    private authStorageService: AuthStorageService,
    private nfsService: NfsService,
    private subvolService: CephfsSubvolumeService,
    private subvolgrpService: CephfsSubvolumeGroupService,
    private route: ActivatedRoute,
    private router: Router,
    private rgwBucketService: RgwBucketService,
    private rgwUserService: RgwUserService,
    private rgwSiteService: RgwSiteService,
    private formBuilder: CdFormBuilder,
    private taskWrapper: TaskWrapperService,
    public actionLabels: ActionLabelsI18n
  ) {
    super();
    this.permission = this.authStorageService.getPermissions().pool;
    this.resource = $localize`NFS export`;
    this.storageBackend = getFsalFromRoute(this.router.url);
  }

  ngOnInit() {
    this.nfsAccessType = this.nfsService.nfsAccessType;
    this.nfsSquash = Object.keys(this.nfsService.nfsSquash);
    this.createForm();
    const promises: Observable<any>[] = [this.nfsService.listClusters()];

    if (this.storageBackend === SUPPORTED_FSAL.RGW) {
      promises.push(this.rgwSiteService.get('realms'));
    } else {
      promises.push(this.nfsService.filesystems());
    }

    if (this.router.url.startsWith(`/${getPathfromFsal(this.storageBackend)}/nfs/edit`)) {
      this.isEdit = true;
    }

    if (this.isEdit) {
      this.action = this.actionLabels.EDIT;
      this.route.params.subscribe(
        (params: { cluster_id: string; export_id: string; rgw_export_type?: string }) => {
          this.cluster_id = decodeURIComponent(params.cluster_id);
          this.export_id = decodeURIComponent(params.export_id);
          if (params.rgw_export_type) {
            this.nfsForm.get('rgw_export_type').setValue(params.rgw_export_type);
            if (params.rgw_export_type === RgwExportType.BUCKET) {
              this.setBucket();
            } else {
              this.setUsers();
            }
          }
          promises.push(this.nfsService.get(this.cluster_id, this.export_id));
          this.getData(promises);
        }
      );
      this.nfsForm.get('cluster_id').disable();
      this.nfsForm.get('fsal').disable();
      this.nfsForm.get('path').disable();
    } else {
      this.action = this.actionLabels.CREATE;
      this.route.params.subscribe(
        (params: { fs_name: string; subvolume_group: string; subvolume?: string }) => {
          this.selectedFsName = params.fs_name;
          this.selectedSubvolGroup = params.subvolume_group;
          if (params.subvolume) this.selectedSubvol = params.subvolume;
        }
      );

      if (this.storageBackend === SUPPORTED_FSAL.RGW) {
        this.nfsForm.get('rgw_export_type').setValue('bucket');
        this.setBucket();
      }
      this.getData(promises);
    }
  }

  getData(promises: Observable<any>[]) {
    forkJoin(promises).subscribe((data: any[]) => {
      this.resolveClusters(data[0]);
      this.resolveFsals(data[1]);
      if (data[2]) {
        this.resolveModel(data[2]);
      }
      this.loadingReady();
    });
  }

  volumeChangeHandler() {
    this.isDefaultSubvolumeGroup();
  }

  async getSubVol() {
    const fs_name = this.nfsForm.getValue('fsal').fs_name;
    const subvolgrp = this.nfsForm.getValue('subvolume_group');
    await this.setSubVolGrpPath();

    (subvolgrp === this.defaultSubVolGroup
      ? this.subvolService.get(fs_name)
      : this.subvolService.get(fs_name, subvolgrp)
    ).subscribe((data: any) => {
      this.allsubvols = data;
    });
    this.setUpVolumeValidation();
  }

  getSubVolGrp(fs_name: string) {
    this.subvolgrpService.get(fs_name).subscribe((data: any) => {
      this.allsubvolgrps = data;
    });
  }

  setSubVolGrpPath(): Promise<void> {
    const fsName = this.nfsForm.getValue('fsal').fs_name;
    const subvolGroup = this.nfsForm.getValue('subvolume_group');

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

  // Checking if subVolGroup is "_nogroup" and updating path to default as "/volumes/_nogroup else blank."
  isDefaultSubvolumeGroup() {
    const fsName = this.nfsForm.getValue('fsal').fs_name;
    this.getSubVolGrp(fsName);
    this.getSubVol();
    this.updatePath('/volumes/' + this.defaultSubVolGroup);
    this.setUpVolumeValidation();
  }

  setSubVolPath(): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      const subvol = this.nfsForm.getValue('subvolume');
      const subvolGroup = this.nfsForm.getValue('subvolume_group');
      const fs_name = this.nfsForm.getValue('fsal').fs_name;

      this.subvolService
        .info(fs_name, subvol, subvolGroup === this.defaultSubVolGroup ? '' : subvolGroup)
        .pipe(map((data) => data['path']))
        .subscribe(
          (path) => {
            this.updatePath(path);
            resolve();
          },
          (error) => reject(error)
        );
    });
  }

  setUpVolumeValidation() {
    const subvolumeGroup = this.nfsForm.get('subvolume_group').value;
    const subVolumeControl = this.nfsForm.get('subvolume');

    // SubVolume is required if SubVolume Group is "_nogroup".
    if (subvolumeGroup == this.defaultSubVolGroup) {
      subVolumeControl?.setValidators([Validators.required]);
    } else {
      subVolumeControl?.clearValidators();
    }
    subVolumeControl?.updateValueAndValidity();
  }

  updatePath(path: string) {
    this.nfsForm.patchValue({ path: path });
  }

  createForm() {
    this.nfsForm = new CdFormGroup({
      // Common fields
      cluster_id: new UntypedFormControl('', {
        validators: [Validators.required]
      }),
      path: new UntypedFormControl('', {
        validators: [
          Validators.required,
          CdValidators.custom('isIsolatedSlash', this.isolatedSlashCondition) // Path can never be single "/".
        ]
      }),
      protocolNfsv3: new UntypedFormControl(true, {
        validators: [
          CdValidators.requiredIf({ protocolNfsv4: false }, (value: boolean) => {
            return !value;
          })
        ]
      }),
      protocolNfsv4: new UntypedFormControl(true, {
        validators: [
          CdValidators.requiredIf({ protocolNfsv3: false }, (value: boolean) => {
            return !value;
          })
        ]
      }),
      pseudo: new UntypedFormControl('', {
        validators: [
          CdValidators.requiredIf({ protocolNfsv4: true, protocolNfsv3: true }),
          Validators.pattern('^/[^><|&()]*$')
        ]
      }),
      access_type: new UntypedFormControl('RW'),
      squash: new UntypedFormControl(this.nfsSquash[0]),
      transportUDP: new UntypedFormControl(true, {
        validators: [
          CdValidators.requiredIf({ transportTCP: false }, (value: boolean) => {
            return !value;
          })
        ]
      }),
      transportTCP: new UntypedFormControl(true, {
        validators: [
          CdValidators.requiredIf({ transportUDP: false }, (value: boolean) => {
            return !value;
          })
        ]
      }),
      clients: this.formBuilder.array([]),
      security_label: new UntypedFormControl(false),

      // FSAL fields (common for RGW and CephFS)
      fsal: new CdFormGroup({
        name: new UntypedFormControl(this.storageBackend, {
          validators: [Validators.required]
        }),
        // RGW-specific field
        user_id: new UntypedFormControl('', {
          validators: [
            CdValidators.requiredIf({
              name: 'RGW'
            })
          ]
        }),
        // CephFS-specific field
        fs_name: new UntypedFormControl('', {
          validators: [
            CdValidators.requiredIf({
              name: 'CEPH'
            })
          ]
        })
      }),

      // CephFS-specific fields
      subvolume_group: new UntypedFormControl(this.defaultSubVolGroup),
      subvolume: new UntypedFormControl(''),
      sec_label_xattr: new UntypedFormControl(
        'security.selinux',
        CdValidators.requiredIf({ security_label: true, 'fsal.name': 'CEPH' })
      ),

      // RGW-specific fields
      rgw_export_type: new UntypedFormControl(
        null,
        CdValidators.requiredIf({
          'fsal.name': 'RGW'
        })
      )
    });
  }

  resolveModel(res: any) {
    if (res.fsal.name === 'CEPH') {
      res.sec_label_xattr = res.fsal.sec_label_xattr;
    }

    res.protocolNfsv4 = res.protocols.indexOf(4) !== -1;
    res.protocolNfsv3 = res.protocols.indexOf(3) !== -1;
    delete res.protocols;

    res.transportTCP = res.transports.indexOf('TCP') !== -1;
    res.transportUDP = res.transports.indexOf('UDP') !== -1;
    delete res.transports;

    Object.entries(this.nfsService.nfsSquash).forEach(([key, value]) => {
      if (value.includes(res.squash)) {
        res.squash = key;
      }
    });

    res.clients.forEach((client: any) => {
      let addressStr = '';
      client.addresses.forEach((address: string) => {
        addressStr += address + ', ';
      });
      if (addressStr.length >= 2) {
        addressStr = addressStr.substring(0, addressStr.length - 2);
      }
      client.addresses = addressStr;
    });

    this.nfsForm.patchValue(res);
    this.setPathValidation();
    this.clients = res.clients;
  }

  resolveClusters(clusters: string[]) {
    this.allClusters = [];
    for (const cluster of clusters) {
      this.allClusters.push({ cluster_id: cluster });
    }
    if (!this.isEdit && this.allClusters.length > 0) {
      this.nfsForm.get('cluster_id').setValue(this.allClusters[0].cluster_id);
    }
  }

  resolveFsals(res: string[]) {
    if (this.storageBackend === SUPPORTED_FSAL.RGW) {
      this.resolveRealms(res);
    } else {
      this.resolveFilesystems(res);
    }
    if (!this.isEdit && this.storageBackend === SUPPORTED_FSAL.RGW) {
      this.nfsForm.patchValue({
        path: '',
        access_type: this.defaultAccessType[SUPPORTED_FSAL.RGW]
      });
    }
  }

  resolveRouteParams() {
    if (!_.isEmpty(this.selectedFsName)) {
      this.nfsForm.patchValue({
        fsal: {
          fs_name: this.selectedFsName
        }
      });
      this.getSubVolGrp(this.selectedFsName);
    }
    if (!_.isEmpty(this.selectedSubvolGroup)) {
      this.nfsForm.patchValue({
        subvolume_group: this.selectedSubvolGroup
      });
      this.getSubVol();
    }
    if (!_.isEmpty(this.selectedSubvol)) {
      this.nfsForm.patchValue({
        subvolume: this.selectedSubvol
      });
      this.setSubVolPath();
    }
  }

  resolveFilesystems(filesystems: any[]) {
    this.allFsNames = filesystems;
    if (!this.isEdit) {
      this.resolveRouteParams();
    }
  }

  resolveRealms(realms: string[]) {
    if (realms.length !== 0) {
      this.rgwSiteService
        .isDefaultRealm()
        .pipe(
          mergeMap((isDefaultRealm) => {
            if (!isDefaultRealm) {
              throw new Error('Selected realm is not the default.');
            }
            return of(true);
          })
        )
        .subscribe({
          error: (error) => {
            const fsalDescr = this.nfsService.nfsFsal.find((f) => f.value === this.storageBackend)
              .descr;
            this.storageBackendError = $localize`${fsalDescr} backend is not available. ${error}`;
          }
        });
    }
  }

  setUsers() {
    this.nfsForm.get('fsal.user_id').enable();
    this.nfsForm.get('path').setValue('');
    this.nfsForm.get('path').disable();
    this.rgwUserService.list().subscribe((users: any) => {
      this.allRGWUsers = users;
    });
  }

  setBucket() {
    this.nfsForm.get('path').enable();
    this.nfsForm.get('fsal.user_id').setValue('');
    this.nfsForm.get('fsal').disable();

    this.setPathValidation();
  }

  onExportTypeChange() {
    this.nfsForm.getValue('rgw_export_type') === RgwExportType.BUCKET
      ? this.setBucket()
      : this.setUsers();
  }

  accessTypeChangeHandler() {
    const name = this.nfsForm.getValue('name');
    const accessType = this.nfsForm.getValue('access_type');
    this.defaultAccessType[name] = accessType;
  }

  isolatedSlashCondition(value: string): boolean {
    return value === '/';
  }

  setPathValidation() {
    const path = this.nfsForm.get('path');
    if (this.storageBackend === SUPPORTED_FSAL.RGW) {
      path.setAsyncValidators([CdValidators.bucketExistence(true, this.rgwBucketService)]);
    } else {
      path.setAsyncValidators([this.pathExistence(true)]);
    }

    if (this.isEdit) {
      path.markAsDirty();
    }
  }

  getAccessTypeHelp(accessType: string) {
    const accessTypeItem = this.nfsAccessType.find((currentAccessTypeItem) => {
      if (accessType === currentAccessTypeItem.value) {
        return currentAccessTypeItem;
      }
    });
    return _.isObjectLike(accessTypeItem) ? accessTypeItem.help : '';
  }

  getId() {
    if (
      _.isString(this.nfsForm.getValue('cluster_id')) &&
      _.isString(this.nfsForm.getValue('path'))
    ) {
      return this.nfsForm.getValue('cluster_id') + ':' + this.nfsForm.getValue('path');
    }
    return '';
  }

  private getPathTypeahead(path: any) {
    if (!_.isString(path) || path === '/') {
      return of([]);
    }

    const fsName = this.nfsForm.getValue('fsal').fs_name;
    return this.nfsService.lsDir(fsName, path).pipe(
      map((result: Directory) =>
        result.paths.filter((dirName: string) => dirName.toLowerCase().includes(path)).slice(0, 15)
      ),
      catchError(() => of([$localize`Error while retrieving paths.`]))
    );
  }

  private getBucketTypeahead(path: string): Observable<any> {
    if (_.isString(path) && path !== '/' && path !== '') {
      return this.rgwBucketService.list().pipe(
        map((bucketList) =>
          bucketList
            .filter((bucketName: string) => bucketName.toLowerCase().includes(path))
            .slice(0, 15)
        ),
        catchError(() => of([$localize`Error while retrieving bucket names.`]))
      );
    } else {
      return of([]);
    }
  }

  submitAction() {
    let action: Observable<any>;
    const requestModel = this.buildRequest();

    if (this.isEdit) {
      action = this.taskWrapper.wrapTaskAroundCall({
        task: new FinishedTask('nfs/edit', {
          cluster_id: this.cluster_id,
          export_id: _.parseInt(this.export_id)
        }),
        call: this.nfsService.update(this.cluster_id, _.parseInt(this.export_id), requestModel)
      });
    } else {
      // Create
      action = this.taskWrapper.wrapTaskAroundCall({
        task: new FinishedTask('nfs/create', {
          path: requestModel.path,
          fsal: requestModel.fsal,
          cluster_id: requestModel.cluster_id
        }),
        call: this.nfsService.create(requestModel)
      });
    }

    action.subscribe({
      error: (errorResponse: CdHttpErrorResponse) => this.setFormErrors(errorResponse),
      complete: () => this.router.navigate([`/${getPathfromFsal(this.storageBackend)}/nfs`])
    });
  }

  private setFormErrors(errorResponse: CdHttpErrorResponse) {
    if (
      errorResponse.error.detail &&
      errorResponse.error.detail
        .toString()
        .includes(`Pseudo ${this.nfsForm.getValue('pseudo')} is already in use`)
    ) {
      this.nfsForm.get('pseudo').setErrors({ pseudoAlreadyExists: true });
    }
    this.nfsForm.setErrors({ cdSubmitButton: true });
  }

  private buildRequest() {
    const requestModel: any = _.cloneDeep(this.nfsForm.value);
    requestModel.fsal = this.nfsForm.get('fsal').value;
    if (this.isEdit) {
      requestModel.export_id = _.parseInt(this.export_id);
      requestModel.path = this.nfsForm.get('path').value;
      if (requestModel.fsal.name === SUPPORTED_FSAL.RGW) {
        requestModel.fsal.user_id = this.nfsForm.getValue('fsal').user_id;
      }
    }

    if (requestModel.fsal.name === SUPPORTED_FSAL.RGW) {
      delete requestModel.fsal.fs_name;
      if (requestModel.rgw_export_type === 'bucket') {
        delete requestModel.fsal.user_id;
      } else {
        requestModel.path = '';
      }
    } else {
      delete requestModel.fsal.user_id;
    }
    delete requestModel.rgw_export_type;
    delete requestModel.subvolume;
    delete requestModel.subvolume_group;

    requestModel.protocols = [];
    if (requestModel.protocolNfsv3) {
      requestModel.protocols.push(3);
    }
    if (requestModel.protocolNfsv4) {
      requestModel.protocols.push(4);
    }
    if (!requestModel.protocolNfsv3 && !requestModel.protocolNfsv4) {
      requestModel.pseudo = null;
    }
    delete requestModel.protocolNfsv3;
    delete requestModel.protocolNfsv4;

    requestModel.transports = [];
    if (requestModel.transportTCP) {
      requestModel.transports.push('TCP');
    }
    delete requestModel.transportTCP;
    if (requestModel.transportUDP) {
      requestModel.transports.push('UDP');
    }
    delete requestModel.transportUDP;

    requestModel.clients.forEach((client: any) => {
      if (_.isString(client.addresses)) {
        client.addresses = _(client.addresses)
          .split(/[ ,]+/)
          .uniq()
          .filter((address) => address !== '')
          .value();
      } else {
        client.addresses = [];
      }

      if (!client.squash) {
        client.squash = requestModel.squash;
      }

      if (!client.access_type) {
        client.access_type = requestModel.access_type;
      }
    });

    if (requestModel.security_label === false || requestModel.fsal.name === 'RGW') {
      requestModel.fsal.sec_label_xattr = null;
    } else {
      requestModel.fsal.sec_label_xattr = requestModel.sec_label_xattr;
    }
    delete requestModel.sec_label_xattr;

    return requestModel;
  }

  private pathExistence(requiredExistenceResult: boolean): AsyncValidatorFn {
    return (control: AbstractControl): Observable<ValidationErrors | null> => {
      if (control.pristine || !control.value) {
        return of({ required: true });
      }
      const fsName = this.nfsForm.getValue('fsal').fs_name;
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
}

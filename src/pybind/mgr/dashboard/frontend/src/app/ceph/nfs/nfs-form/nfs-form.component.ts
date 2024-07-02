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

  storageBackend: SUPPORTED_FSAL;
  storageBackendError: string = null;

  defaultAccessType = { RGW: 'RO' };
  nfsAccessType: any[] = [];
  nfsSquash: any[] = [];

  action: string;
  resource: string;

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
    private route: ActivatedRoute,
    private router: Router,
    private rgwBucketService: RgwBucketService,
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

    if (this.storageBackend === 'RGW') {
      promises.push(this.rgwSiteService.get('realms'));
    } else {
      promises.push(this.nfsService.filesystems());
    }

    if (this.router.url.startsWith(`/${getPathfromFsal(this.storageBackend)}/nfs/edit`)) {
      this.isEdit = true;
    }

    if (this.isEdit) {
      this.action = this.actionLabels.EDIT;
      this.route.params.subscribe((params: { cluster_id: string; export_id: string }) => {
        this.cluster_id = decodeURIComponent(params.cluster_id);
        this.export_id = decodeURIComponent(params.export_id);
        promises.push(this.nfsService.get(this.cluster_id, this.export_id));
        this.getData(promises);
      });
      this.nfsForm.get('cluster_id').disable();
    } else {
      this.action = this.actionLabels.CREATE;
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

  createForm() {
    this.nfsForm = new CdFormGroup({
      cluster_id: new UntypedFormControl('', {
        validators: [Validators.required]
      }),
      fsal: new CdFormGroup({
        name: new UntypedFormControl(this.storageBackend, {
          validators: [Validators.required]
        }),
        fs_name: new UntypedFormControl('', {
          validators: [
            CdValidators.requiredIf({
              name: 'CEPH'
            })
          ]
        })
      }),
      path: new UntypedFormControl('/', {
        validators: [Validators.required]
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
      sec_label_xattr: new UntypedFormControl(
        'security.selinux',
        CdValidators.requiredIf({ security_label: true, 'fsal.name': 'CEPH' })
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
    if (this.storageBackend === 'RGW') {
      this.setPathValidation();
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

  resolveFilesystems(filesystems: any[]) {
    this.allFsNames = filesystems;
    if (!this.isEdit && filesystems.length > 0) {
      this.nfsForm.patchValue({
        fsal: {
          fs_name: filesystems[0].name
        }
      });
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

  accessTypeChangeHandler() {
    const name = this.nfsForm.getValue('name');
    const accessType = this.nfsForm.getValue('access_type');
    this.defaultAccessType[name] = accessType;
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

  pathChangeHandler() {
    if (!this.isEdit) {
      this.nfsForm.patchValue({
        pseudo: this.generatePseudo()
      });
    }
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

  private generatePseudo() {
    let newPseudo = this.nfsForm.getValue('pseudo');
    if (this.nfsForm.get('pseudo') && !this.nfsForm.get('pseudo').dirty) {
      newPseudo = undefined;
      if (this.storageBackend === 'CEPH') {
        newPseudo = '/cephfs';
        if (_.isString(this.nfsForm.getValue('path'))) {
          newPseudo += this.nfsForm.getValue('path');
        }
      }
    }
    return newPseudo;
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

    if (this.isEdit) {
      requestModel.export_id = _.parseInt(this.export_id);
    }

    if (requestModel.fsal.name === 'RGW') {
      delete requestModel.fsal.fs_name;
    }

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

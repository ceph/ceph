import { ChangeDetectorRef, Component, OnInit, ViewChild } from '@angular/core';
import { FormControl, Validators } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';

import { I18n } from '@ngx-translate/i18n-polyfill';
import * as _ from 'lodash';
import { forkJoin, Observable, of } from 'rxjs';
import { debounceTime, distinctUntilChanged, map, mergeMap } from 'rxjs/operators';

import { NfsService } from '../../../shared/api/nfs.service';
import { RgwUserService } from '../../../shared/api/rgw-user.service';
import { SelectMessages } from '../../../shared/components/select/select-messages.model';
import { SelectOption } from '../../../shared/components/select/select-option.model';
import { ActionLabelsI18n } from '../../../shared/constants/app.constants';
import { Icons } from '../../../shared/enum/icons.enum';
import { CdForm } from '../../../shared/forms/cd-form';
import { CdFormBuilder } from '../../../shared/forms/cd-form-builder';
import { CdFormGroup } from '../../../shared/forms/cd-form-group';
import { CdValidators } from '../../../shared/forms/cd-validators';
import { FinishedTask } from '../../../shared/models/finished-task';
import { Permission } from '../../../shared/models/permissions';
import { CephReleaseNamePipe } from '../../../shared/pipes/ceph-release-name.pipe';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';
import { SummaryService } from '../../../shared/services/summary.service';
import { TaskWrapperService } from '../../../shared/services/task-wrapper.service';
import { NfsFormClientComponent } from '../nfs-form-client/nfs-form-client.component';

@Component({
  selector: 'cd-nfs-form',
  templateUrl: './nfs-form.component.html',
  styleUrls: ['./nfs-form.component.scss']
})
export class NfsFormComponent extends CdForm implements OnInit {
  @ViewChild('nfsClients', { static: true })
  nfsClients: NfsFormClientComponent;

  permission: Permission;
  nfsForm: CdFormGroup;
  isEdit = false;

  cluster_id: string = null;
  export_id: string = null;

  isNewDirectory = false;
  isNewBucket = false;
  isDefaultCluster = false;

  allClusters: string[] = null;
  allDaemons = {};
  icons = Icons;

  allFsals: any[] = [];
  allRgwUsers: any[] = [];
  allCephxClients: any[] = null;
  allFsNames: any[] = null;

  defaultAccessType = { RGW: 'RO' };
  nfsAccessType: any[] = this.nfsService.nfsAccessType;
  nfsSquash: any[] = this.nfsService.nfsSquash;

  action: string;
  resource: string;
  docsUrl: string;

  daemonsSelections: SelectOption[] = [];
  daemonsMessages = new SelectMessages(
    { noOptions: this.i18n('There are no daemons available.') },
    this.i18n
  );

  pathDataSource = (text$: Observable<string>) => {
    return text$.pipe(
      debounceTime(200),
      distinctUntilChanged(),
      mergeMap((token: string) => this.getPathTypeahead(token)),
      map((val: any) => val.paths)
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
    private rgwUserService: RgwUserService,
    private formBuilder: CdFormBuilder,
    private summaryservice: SummaryService,
    private cephReleaseNamePipe: CephReleaseNamePipe,
    private taskWrapper: TaskWrapperService,
    private cdRef: ChangeDetectorRef,
    private i18n: I18n,
    public actionLabels: ActionLabelsI18n
  ) {
    super();
    this.permission = this.authStorageService.getPermissions().pool;
    this.resource = this.i18n('NFS export');
    this.createForm();
  }

  ngOnInit() {
    const promises: Observable<any>[] = [
      this.nfsService.daemon(),
      this.nfsService.fsals(),
      this.nfsService.clients(),
      this.nfsService.filesystems()
    ];

    if (this.router.url.startsWith('/nfs/edit')) {
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
    } else {
      this.action = this.actionLabels.CREATE;
      this.getData(promises);
    }

    this.summaryservice.subscribeOnce((summary) => {
      const releaseName = this.cephReleaseNamePipe.transform(summary.version);
      this.docsUrl = `http://docs.ceph.com/docs/${releaseName}/radosgw/nfs/`;
    });
  }

  getData(promises: Observable<any>[]) {
    forkJoin(promises).subscribe((data: any[]) => {
      this.resolveDaemons(data[0]);
      this.resolvefsals(data[1]);
      this.resolveClients(data[2]);
      this.resolveFilesystems(data[3]);
      if (data[4]) {
        this.resolveModel(data[4]);
      }

      this.loadingReady();
    });
  }

  createForm() {
    this.nfsForm = new CdFormGroup({
      cluster_id: new FormControl('', {
        validators: [Validators.required]
      }),
      daemons: new FormControl([]),
      fsal: new CdFormGroup({
        name: new FormControl('', {
          validators: [Validators.required]
        }),
        user_id: new FormControl('', {
          validators: [
            CdValidators.requiredIf({
              name: 'CEPH'
            })
          ]
        }),
        fs_name: new FormControl('', {
          validators: [
            CdValidators.requiredIf({
              name: 'CEPH'
            })
          ]
        }),
        rgw_user_id: new FormControl('', {
          validators: [
            CdValidators.requiredIf({
              name: 'RGW'
            })
          ]
        })
      }),
      path: new FormControl(''),
      protocolNfsv3: new FormControl(true, {
        validators: [
          CdValidators.requiredIf({ protocolNfsv4: false }, (value: boolean) => {
            return !value;
          })
        ]
      }),
      protocolNfsv4: new FormControl(true, {
        validators: [
          CdValidators.requiredIf({ protocolNfsv3: false }, (value: boolean) => {
            return !value;
          })
        ]
      }),
      tag: new FormControl(''),
      pseudo: new FormControl('', {
        validators: [
          CdValidators.requiredIf({ protocolNfsv4: true }),
          Validators.pattern('^/[^><|&()]*$')
        ]
      }),
      access_type: new FormControl('RW', {
        validators: [Validators.required]
      }),
      squash: new FormControl('', {
        validators: [Validators.required]
      }),
      transportUDP: new FormControl(true, {
        validators: [
          CdValidators.requiredIf({ transportTCP: false }, (value: boolean) => {
            return !value;
          })
        ]
      }),
      transportTCP: new FormControl(true, {
        validators: [
          CdValidators.requiredIf({ transportUDP: false }, (value: boolean) => {
            return !value;
          })
        ]
      }),
      clients: this.formBuilder.array([]),
      security_label: new FormControl(false),
      sec_label_xattr: new FormControl(
        'security.selinux',
        CdValidators.requiredIf({ security_label: true, 'fsal.name': 'CEPH' })
      )
    });
  }

  resolveModel(res: any) {
    if (res.fsal.name === 'CEPH') {
      res.sec_label_xattr = res.fsal.sec_label_xattr;
    }

    this.daemonsSelections = _.map(
      this.allDaemons[res.cluster_id],
      (daemon) => new SelectOption(res.daemons.indexOf(daemon) !== -1, daemon, '')
    );
    this.daemonsSelections = [...this.daemonsSelections];

    res.protocolNfsv3 = res.protocols.indexOf(3) !== -1;
    res.protocolNfsv4 = res.protocols.indexOf(4) !== -1;
    delete res.protocols;

    res.transportTCP = res.transports.indexOf('TCP') !== -1;
    res.transportUDP = res.transports.indexOf('UDP') !== -1;
    delete res.transports;

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
    this.nfsClients.resolveModel(res.clients);
  }

  resolveDaemons(daemons: Record<string, any>) {
    daemons = _.sortBy(daemons, ['daemon_id']);

    this.allClusters = _(daemons)
      .map((daemon) => daemon.cluster_id)
      .sortedUniq()
      .value();

    _.forEach(this.allClusters, (cluster) => {
      this.allDaemons[cluster] = [];
    });

    _.forEach(daemons, (daemon) => {
      this.allDaemons[daemon.cluster_id].push(daemon.daemon_id);
    });

    const hasOneCluster = _.isArray(this.allClusters) && this.allClusters.length === 1;
    this.isDefaultCluster = hasOneCluster && this.allClusters[0] === '_default_';
    if (hasOneCluster) {
      this.nfsForm.patchValue({
        cluster_id: this.allClusters[0]
      });
      this.onClusterChange();
    }
  }

  resolvefsals(res: string[]) {
    res.forEach((fsal) => {
      const fsalItem = this.nfsService.nfsFsal.find((currentFsalItem) => {
        return fsal === currentFsalItem.value;
      });

      if (_.isObjectLike(fsalItem)) {
        this.allFsals.push(fsalItem);
        if (fsalItem.value === 'RGW') {
          this.rgwUserService.list().subscribe((result: any) => {
            result.forEach((user: Record<string, any>) => {
              if (user.suspended === 0 && user.keys.length > 0) {
                this.allRgwUsers.push(user.user_id);
              }
            });
          });
        }
      }
    });

    if (this.allFsals.length === 1 && _.isUndefined(this.nfsForm.getValue('fsal'))) {
      this.nfsForm.patchValue({
        fsal: this.allFsals[0]
      });
    }
  }

  resolveClients(clients: any[]) {
    this.allCephxClients = clients;
  }

  resolveFilesystems(filesystems: any[]) {
    this.allFsNames = filesystems;
    if (filesystems.length === 1) {
      this.nfsForm.patchValue({
        fsal: {
          fs_name: filesystems[0].name
        }
      });
    }
  }

  fsalChangeHandler() {
    this.nfsForm.patchValue({
      tag: this._generateTag(),
      pseudo: this._generatePseudo(),
      access_type: this._updateAccessType()
    });

    this.setPathValidation();

    this.cdRef.detectChanges();
  }

  accessTypeChangeHandler() {
    const name = this.nfsForm.getValue('name');
    const accessType = this.nfsForm.getValue('access_type');
    this.defaultAccessType[name] = accessType;
  }

  setPathValidation() {
    if (this.nfsForm.getValue('name') === 'RGW') {
      this.nfsForm
        .get('path')
        .setValidators([Validators.required, Validators.pattern('^(/|[^/><|&()#?]+)$')]);
    } else {
      this.nfsForm
        .get('path')
        .setValidators([Validators.required, Validators.pattern('^/[^><|&()?]*$')]);
    }
  }

  rgwUserIdChangeHandler() {
    this.nfsForm.patchValue({
      pseudo: this._generatePseudo()
    });
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

  getPathTypeahead(path: any) {
    if (!_.isString(path) || path === '/') {
      return of([]);
    }

    return this.nfsService.lsDir(path);
  }

  pathChangeHandler() {
    this.nfsForm.patchValue({
      pseudo: this._generatePseudo()
    });

    const path = this.nfsForm.getValue('path');
    this.getPathTypeahead(path).subscribe((res: any) => {
      this.isNewDirectory = path !== '/' && res.paths.indexOf(path) === -1;
    });
  }

  bucketChangeHandler() {
    this.nfsForm.patchValue({
      tag: this._generateTag(),
      pseudo: this._generatePseudo()
    });

    const bucket = this.nfsForm.getValue('path');
    this.getBucketTypeahead(bucket).subscribe((res: any) => {
      this.isNewBucket = bucket !== '' && res.indexOf(bucket) === -1;
    });
  }

  getBucketTypeahead(path: string): Observable<any> {
    const rgwUserId = this.nfsForm.getValue('rgw_user_id');

    if (_.isString(rgwUserId) && _.isString(path) && path !== '/' && path !== '') {
      return this.nfsService.buckets(rgwUserId);
    } else {
      return of([]);
    }
  }

  _generateTag() {
    let newTag = this.nfsForm.getValue('tag');
    if (!this.nfsForm.get('tag').dirty) {
      newTag = undefined;
      if (this.nfsForm.getValue('fsal') === 'RGW') {
        newTag = this.nfsForm.getValue('path');
      }
    }
    return newTag;
  }

  _generatePseudo() {
    let newPseudo = this.nfsForm.getValue('pseudo');
    if (this.nfsForm.get('pseudo') && !this.nfsForm.get('pseudo').dirty) {
      newPseudo = undefined;
      if (this.nfsForm.getValue('fsal') === 'CEPH') {
        newPseudo = '/cephfs';
        if (_.isString(this.nfsForm.getValue('path'))) {
          newPseudo += this.nfsForm.getValue('path');
        }
      } else if (this.nfsForm.getValue('fsal') === 'RGW') {
        if (_.isString(this.nfsForm.getValue('rgw_user_id'))) {
          newPseudo = '/' + this.nfsForm.getValue('rgw_user_id');
          if (_.isString(this.nfsForm.getValue('path'))) {
            newPseudo += '/' + this.nfsForm.getValue('path');
          }
        }
      }
    }
    return newPseudo;
  }

  _updateAccessType() {
    const name = this.nfsForm.getValue('name');
    let accessType = this.defaultAccessType[name];

    if (!accessType) {
      accessType = 'RW';
    }

    return accessType;
  }

  onClusterChange() {
    const cluster_id = this.nfsForm.getValue('cluster_id');
    this.daemonsSelections = _.map(
      this.allDaemons[cluster_id],
      (daemon) => new SelectOption(false, daemon, '')
    );
    this.daemonsSelections = [...this.daemonsSelections];
    this.nfsForm.patchValue({ daemons: [] });
  }

  removeDaemon(index: number, daemon: string) {
    this.daemonsSelections.forEach((value) => {
      if (value.name === daemon) {
        value.selected = false;
      }
    });

    const daemons = this.nfsForm.get('daemons');
    daemons.value.splice(index, 1);
    daemons.setValue(daemons.value);

    return false;
  }

  onDaemonSelection() {
    this.nfsForm.get('daemons').setValue(this.nfsForm.getValue('daemons'));
  }

  submitAction() {
    let action: Observable<any>;
    const requestModel = this._buildRequest();

    if (this.isEdit) {
      action = this.taskWrapper.wrapTaskAroundCall({
        task: new FinishedTask('nfs/edit', {
          cluster_id: this.cluster_id,
          export_id: this.export_id
        }),
        call: this.nfsService.update(this.cluster_id, this.export_id, requestModel)
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
      error: () => this.nfsForm.setErrors({ cdSubmitButton: true }),
      complete: () => this.router.navigate(['/nfs'])
    });
  }

  _buildRequest() {
    const requestModel: any = _.cloneDeep(this.nfsForm.value);

    if (_.isUndefined(requestModel.tag) || requestModel.tag === '') {
      requestModel.tag = null;
    }

    if (this.isEdit) {
      requestModel.export_id = this.export_id;
    }

    if (requestModel.fsal.name === 'CEPH') {
      delete requestModel.fsal.rgw_user_id;
    } else {
      delete requestModel.fsal.fs_name;
      delete requestModel.fsal.user_id;
    }

    requestModel.protocols = [];
    if (requestModel.protocolNfsv3) {
      requestModel.protocols.push(3);
    } else {
      requestModel.tag = null;
    }
    delete requestModel.protocolNfsv3;
    if (requestModel.protocolNfsv4) {
      requestModel.protocols.push(4);
    } else {
      requestModel.pseudo = null;
    }
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
}

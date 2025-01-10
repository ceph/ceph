import { ChangeDetectorRef, Component, OnInit } from '@angular/core';
import { Router } from '@angular/router';
import { forkJoin, Observable } from 'rxjs';
import { map } from 'rxjs/operators';

import _ from 'lodash';
import {
  AUTHMODE,
  CLUSTERING,
  PLACEMENT,
  RESOURCE,
  DomainSettings,
  JoinSource,
  CLUSTER_RESOURCE,
  ClusterRequestModel,
  SMBUsersGroups,
  PublicAddress
} from '../smb.model';
import { ActionLabelsI18n, URLVerbs } from '~/app/shared/constants/app.constants';
import { Icons } from '~/app/shared/enum/icons.enum';

import { FormArray, FormControl, UntypedFormControl, Validators } from '@angular/forms';
import { CdForm } from '~/app/shared/forms/cd-form';
import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { FinishedTask } from '~/app/shared/models/finished-task';

import { OrchestratorService } from '~/app/shared/api/orchestrator.service';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { HostService } from '~/app/shared/api/host.service';
import { SmbService } from '~/app/shared/api/smb.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { SmbDomainSettingModalComponent } from '../smb-domain-setting-modal/smb-domain-setting-modal.component';
import { CephServicePlacement } from '~/app/shared/models/service.interface';
import { USERSGROUPS_URL } from '../smb-usersgroups-list/smb-usersgroups-list.component';

@Component({
  selector: 'cd-smb-cluster-form',
  templateUrl: './smb-cluster-form.component.html',
  styleUrls: ['./smb-cluster-form.component.scss']
})
export class SmbClusterFormComponent extends CdForm implements OnInit {
  smbForm: CdFormGroup;
  hostsAndLabels$: Observable<{ hosts: any[]; labels: any[] }>;
  hasOrchestrator: boolean;
  orchStatus$: Observable<any>;
  allClustering: string[] = [];
  CLUSTERING = CLUSTERING;
  selectedLabels: string[] = [];
  selectedHosts: string[] = [];
  action: string;
  resource: string;
  icons = Icons;
  domainSettingsObject: DomainSettings;
  modalData$!: Observable<DomainSettings>;
  usersGroups$: Observable<SMBUsersGroups[]>;

  constructor(
    private hostService: HostService,
    private formBuilder: CdFormBuilder,
    public smbService: SmbService,
    public actionLabels: ActionLabelsI18n,
    private orchService: OrchestratorService,
    private modalService: ModalCdsService,
    private taskWrapperService: TaskWrapperService,
    private router: Router,
    private cd: ChangeDetectorRef
  ) {
    super();
    this.resource = $localize`Cluster`;
    this.modalData$ = this.smbService.modalData$;
  }
  ngOnInit() {
    this.action = this.actionLabels.CREATE;
    this.usersGroups$ = this.smbService.listUsersGroups();
    this.smbService.modalData$.subscribe((data: DomainSettings) => {
      this.domainSettingsObject = data;
      this.smbForm.get('domain_settings').setValue(data?.realm);
    });
    this.createForm();
    this.hostsAndLabels$ = forkJoin({
      hosts: this.hostService.getAllHosts(),
      labels: this.hostService.getLabels()
    }).pipe(
      map(({ hosts, labels }) => ({
        hosts: hosts.map((host: any) => ({ content: host['hostname'] })),
        labels: labels.map((label: string) => ({ content: label }))
      }))
    );
    this.orchStatus$ = this.orchService.status();
    this.allClustering = Object.values(CLUSTERING);
    this.onAuthModeChange();
  }

  createForm() {
    this.smbForm = this.formBuilder.group({
      cluster_id: new FormControl('', {
        validators: [Validators.required]
      }),
      auth_mode: [
        AUTHMODE.activeDirectory,
        {
          validators: [Validators.required]
        }
      ],
      domain_settings: [null],
      placement: [{}],
      hosts: [[]],
      label: [
        null,
        [
          CdValidators.requiredIf({
            placement: 'label'
          })
        ]
      ],
      count: [1],
      custom_dns: new FormArray([]),
      joinSources: new FormArray([]),
      clustering: new UntypedFormControl(
        CLUSTERING.Default.charAt(0).toUpperCase() + CLUSTERING.Default.slice(1)
      ),
      public_addrs: new FormArray([])
    });

    this.orchService.status().subscribe((status) => {
      this.hasOrchestrator = status.available;
      this.smbForm.get('placement').setValue(this.hasOrchestrator ? PLACEMENT.host : '');
    });
  }

  multiSelector(event: any, field: 'label' | 'hosts') {
    if (field === PLACEMENT.host) this.selectedLabels = event.map((label: any) => label.content);
    else this.selectedHosts = event.map((host: any) => host.content);
  }

  onAuthModeChange() {
    const authMode = this.smbForm.get('auth_mode').value;
    const domainSettingsControl = this.smbForm.get('domain_settings');
    const userGroupSettingsControl = this.smbForm.get('joinSources') as FormArray;

    // User Group Setting should be optional if authMode is "Active Directory"
    if (authMode === AUTHMODE.activeDirectory) {
      if (userGroupSettingsControl) {
        userGroupSettingsControl.clear();
      }
      if (domainSettingsControl) {
        domainSettingsControl.setValidators(Validators.required);
        domainSettingsControl.updateValueAndValidity();
      }
      if (userGroupSettingsControl) {
        userGroupSettingsControl.clearValidators();
        userGroupSettingsControl.updateValueAndValidity();
      }
      // Domain Setting should be optional if authMode is "Users"
    } else if (authMode === AUTHMODE.User) {
      const control = new FormControl(null, Validators.required);
      userGroupSettingsControl.push(control);
      domainSettingsControl.setErrors(null);
      domainSettingsControl.clearValidators();
      userGroupSettingsControl.setValidators(Validators.required);
    } else {
      if (userGroupSettingsControl) {
        userGroupSettingsControl.clearValidators();
        userGroupSettingsControl.clear();
        userGroupSettingsControl.updateValueAndValidity();
      }
    }
  }

  submitAction() {
    const domainSettingsControl = this.smbForm.get('domain_settings');
    const authMode = this.smbForm.get('auth_mode').value;

    // Domain Setting should be mandatory if authMode is "Active Directory"
    if (authMode === AUTHMODE.activeDirectory && !domainSettingsControl.value) {
      domainSettingsControl.setErrors({ required: true });
      this.smbForm.markAllAsTouched();
      return;
    }
    const component = this;
    const requestModel = this.buildRequest();
    const BASE_URL = 'smb/cluster';
    const cluster_id = this.smbForm.get('cluster_id').value;
    const taskUrl = `${BASE_URL}/${URLVerbs.CREATE}`;
    this.taskWrapperService
      .wrapTaskAroundCall({
        task: new FinishedTask(taskUrl, { cluster_id }),
        call: this.smbService.createCluster(requestModel)
      })
      .subscribe({
        complete: () => {
          this.router.navigate([`cephfs/smb`]);
        },
        error() {
          component.smbForm.setErrors({ cdSubmitButton: true });
        }
      });
  }

  private buildRequest() {
    const values = this.smbForm.getRawValue();
    const rawFormValue = _.cloneDeep(this.smbForm.value);
    const joinSources: JoinSource[] = (this.domainSettingsObject?.join_sources || [])
      .filter((source: { ref: string }) => source.ref)
      .map((source: { ref: string }) => ({
        ref: source.ref,
        source_type: RESOURCE.Resource
      }));

    const joinSourceObj = joinSources.map((source: JoinSource) => ({
      source_type: RESOURCE.Resource,
      ref: source.ref
    }));

    const domainSettings = {
      realm: this.domainSettingsObject?.realm,
      join_sources: joinSourceObj
    };

    const requestModel: ClusterRequestModel = {
      cluster_resource: {
        resource_type: CLUSTER_RESOURCE,
        cluster_id: rawFormValue.cluster_id,
        auth_mode: rawFormValue.auth_mode
      }
    };

    if (domainSettings && domainSettings.join_sources.length > 0) {
      requestModel.cluster_resource.domain_settings = domainSettings;
    }
    if (rawFormValue.joinSources?.length > 0) {
      requestModel.cluster_resource.user_group_settings = rawFormValue.joinSources.map(
        (source: { ref: string }) => ({
          source_type: RESOURCE.Resource,
          ref: source
        })
      );
    }

    const serviceSpec = this.getPlacementSpec(values);
    if (serviceSpec) {
      requestModel.cluster_resource.placement = serviceSpec;
    }

    if (rawFormValue.custom_dns?.length > 0) {
      requestModel.cluster_resource.custom_dns = rawFormValue.custom_dns;
    }

    if (rawFormValue.public_addrs?.length > 0) {
      requestModel.cluster_resource.public_addrs = rawFormValue.public_addrs.map(
        (publicAddress: PublicAddress) => {
          return publicAddress.destination ? publicAddress : { address: publicAddress.address };
        }
      );
    }

    if (rawFormValue.clustering && rawFormValue.clustering.toLowerCase() !== CLUSTERING.Default) {
      requestModel.cluster_resource.clustering = rawFormValue.clustering.toLowerCase();
    }

    return requestModel;
  }

  getPlacementSpec(values: CephServicePlacement) {
    const serviceSpec = {
      placement: {}
    };

    serviceSpec['placement']['count'] = values.count;

    switch (values['placement']) {
      case 'hosts':
        if (values['hosts'].length > 0) {
          serviceSpec['placement']['hosts'] = this.selectedHosts;
          serviceSpec['placement']['count'] = values.count;
        }
        break;
      case 'label':
        serviceSpec['placement']['label'] = this.selectedLabels;
        serviceSpec['placement']['count'] = values.count;
        break;
    }

    return serviceSpec.placement;
  }

  editDomainSettingsModal() {
    this.modalService.show(SmbDomainSettingModalComponent, {
      domainSettingsObject: this.domainSettingsObject
    });
  }

  deleteDomainSettingsModal() {
    this.smbForm.get('domain_settings')?.setValue('');
    this.domainSettingsObject = { realm: '', join_sources: [] };
  }

  get joinSources() {
    return this.smbForm.get('joinSources') as FormArray;
  }

  get custom_dns() {
    return this.smbForm.get('custom_dns') as FormArray;
  }

  get public_addrs() {
    return this.smbForm.get('public_addrs') as FormArray;
  }

  addUserGroupSetting() {
    const control = new FormControl(null, Validators.required);
    this.joinSources.push(control);
  }

  navigateCreateUsersGroups() {
    this.router.navigate([`${USERSGROUPS_URL}/${URLVerbs.CREATE}`]);
  }

  addCustomDns() {
    const control = new FormControl('', Validators.required);
    this.custom_dns.push(control);
  }

  addPublicAddrs() {
    const control = this.formBuilder.group({
      address: ['', Validators.required],
      destination: ['']
    });
    this.public_addrs.push(control);
  }

  removeUserGroupSetting(index: number) {
    this.joinSources.removeAt(index);
    this.cd.detectChanges();
  }

  removeCustomDNS(index: number) {
    this.custom_dns.removeAt(index);
    this.cd.detectChanges();
  }

  removePublicAddrs(index: number) {
    this.public_addrs.removeAt(index);
    this.cd.detectChanges();
  }
}

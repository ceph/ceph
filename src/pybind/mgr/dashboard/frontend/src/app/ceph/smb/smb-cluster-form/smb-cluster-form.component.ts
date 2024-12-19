import { Component, OnInit } from '@angular/core';
import { Router } from '@angular/router';
import { forkJoin, Observable } from 'rxjs';
import { map } from 'rxjs/operators';

import _ from 'lodash';
import { Clustering, Resource_Type } from '../smb.model';
import { AuthMode } from '../smb.model';
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
  PUBLICADDRPATTERN = /^(\d{1,3}\.){3}\d{1,3}\/\d{1,2}(\%[a-zA-Z0-9-]+)?$/;
  allClustering: string[] = [];
  selectedLabels: string[] = [];
  selectedHosts: string[] = [];
  action: any;
  resource: string;
  icons = Icons;
  domainSettingsObject: any;
  modalData$ = this.smbService.modalData$;

  constructor(
    private hostService: HostService,
    private formBuilder: CdFormBuilder,
    public smbService: SmbService,
    public actionLabels: ActionLabelsI18n,
    private orchService: OrchestratorService,
    private modalService: ModalCdsService,
    private taskWrapperService: TaskWrapperService,
    private router: Router
  ) {
    super();
    this.resource = $localize`Cluster`;
  }
  ngOnInit() {
    this.action = this.actionLabels.CREATE;
    this.smbService.modalData$.subscribe((data) => {
      this.domainSettingsObject = data;
      this.smbForm.get('domain_settings').setValue(data?.realm);
    });
    this.createForm();

    this.action = this.actionLabels.CREATE;
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
    this.allClustering = Object.values(Clustering);
  }

  createForm() {
    this.smbForm = this.formBuilder.group({
      cluster_id: new FormControl('', {
        validators: [Validators.required]
      }),
      auth_mode: [
        '',
        {
          validators: [Validators.required]
        }
      ],
      domain_settings: [''],
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
      custom_dns: new FormArray([]),
      joinSources: new FormArray([]),
      clustering: new UntypedFormControl(Clustering.Default)
    });

    this.orchService.status().subscribe((status) => {
      this.hasOrchestrator = status.available;
      this.smbForm.get('placement').setValue(this.hasOrchestrator ? 'hosts' : '');
    });
  }

  multiSelector(event: any, field: 'label' | 'hosts') {
    if (field === 'label') this.selectedLabels = event.map((label: any) => label.content);
    else this.selectedHosts = event.map((host: any) => host.content);
  }

  onAuthModeChange(): void {
    const authMode = this.smbForm.get('auth_mode').value;
    const domainSettingsControl = this.smbForm.get('domain_settings');
    const userGroupSettingsControl = this.smbForm.get('joinSources');

    if (authMode === AuthMode.activeDirectory) {
      const joinSources = this.smbForm.get('joinSources') as FormArray;
      if (joinSources) {
        joinSources.clear();
      }
      console.log(userGroupSettingsControl);
      domainSettingsControl.setValidators(Validators.required);
      userGroupSettingsControl.clearValidators();
    } else if (authMode === AuthMode.User) {
      const control = new FormControl('', Validators.required);
    this.joinSources.push(control);
      userGroupSettingsControl.setValidators(Validators.required);
      domainSettingsControl.clearValidators();
    }
    domainSettingsControl.updateValueAndValidity();
    userGroupSettingsControl.updateValueAndValidity();
  }

  submitAction() {
    console.log(this.smbForm, 'form');
    const requestModel = this.buildRequest();
    let values = this.smbForm.getRawValue();
    const serviceSpec: object = {
      placement: {}
    };
    switch (values['placement']) {
      case 'hosts':
        if (values['hosts'].length > 0) {
          serviceSpec['placement']['hosts'] = this.selectedHosts;
        }
        break;
      case 'label':
        serviceSpec['placement']['label'] = this.selectedLabels;
        break;
    }

    const BASE_URL = 'smb/cluster';
    const cluster_id = this.smbForm.get('cluster_id').value;
    const taskUrl = `${BASE_URL}/${URLVerbs.CREATE}`;
    this.taskWrapperService
      .wrapTaskAroundCall({
        task: new FinishedTask(taskUrl, { cluster_id }),
        call: this.smbService.create(requestModel)
      })
      .subscribe({
        complete: () => {
          this.router.navigate([`cephfs/smb`]);
        }
      });
  }

  private buildRequest() {
    const rawFormValue = _.cloneDeep(this.smbForm.value);
    console.log(this.domainSettingsObject, this.joinSources, 'te');
    const joinSources = (this.domainSettingsObject?.joinSources || [])
      .filter((source: any) => source.ref)
      .map((source: any) => ({ ref: source.ref }));

    console.log(joinSources, 'joinSources');

    const domainSettings =
      this.domainSettingsObject?.realm || joinSources.length > 0
        ? {
            realm: this.domainSettingsObject?.realm,
            join_sources: joinSources.length > 0 ? joinSources : undefined
          }
        : undefined;

    const requestModel: any = {
      cluster_resource: {
        resource_type: Resource_Type,
        cluster_id: rawFormValue.cluster_id,
        auth_mode: rawFormValue.auth_mode
      }
    };
    if (this.domainSettingsObject?.realm || joinSources.length > 0) {
      requestModel.cluster_resource.domain_settings = domainSettings;
    }

    if (rawFormValue.joinSources?.length > 0) {
      requestModel.cluster_resource.user_group_setting = rawFormValue.joinSources;
    }

    if (rawFormValue.placement && Object.keys(rawFormValue.placement).length > 0) {
      // requestModel.cluster_resource.placement = rawFormValue.placement;
    }

    if (rawFormValue.custom_dns?.length > 0) {
      requestModel.cluster_resource.custom_dns = rawFormValue.custom_dns;
    }

    if (rawFormValue.clustering && rawFormValue.clustering.trim() !== '') {
      requestModel.cluster_resource.clustering = rawFormValue.clustering;
    }

    return requestModel;
  }

  editDomainSettingsModal() {
   // const realm = this.smbForm.get('domain_settings').value;
    console.log(this.smbForm.get('domain_settings'), this.domainSettingsObject);
    this.modalService.show(SmbDomainSettingModalComponent, {
      domainSettingsObject : this.domainSettingsObject
    });
  }

  deleteDomainSettingsModal() {
    console.log('jjh');
    this.smbForm.get('domain_settings')?.setValue('');
    console.log(this.smbForm.get('domain_settings')?.setValue(''));
    console.log(this.smbForm);
  }

  get joinSources() {
    return this.smbForm.get('joinSources') as FormArray;
  }

  get custom_dns() {
    return this.smbForm.get('custom_dns') as FormArray;
  }

  addUserGroupSetting() {
    const control = new FormControl('', Validators.required);
    this.joinSources.push(control);
  }

  addCustomDns() {
    const control = new FormControl('', Validators.required);
    this.custom_dns.push(control);
  }

  removeUserGroupSetting(index: number) {
    this.joinSources.removeAt(index);
  }

  removeCustomDNS(index: number) {
    this.custom_dns.removeAt(index);
  }
}

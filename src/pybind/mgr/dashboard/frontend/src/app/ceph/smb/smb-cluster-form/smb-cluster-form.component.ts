import { Component, OnInit } from '@angular/core';
import { Router } from '@angular/router';
import { forkJoin, Observable } from 'rxjs';
import { map } from 'rxjs/operators';

import _ from 'lodash';
import { AuthMode, Clustering, Resource_Type, Resources } from '../smb.model';
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
  allClustering: string[] = [];
  selectedLabels: string[] = [];
  selectedHosts: string[] = [];
  action: string;
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
      domain_settings: [{ value: '', disabled: true }],
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
      count: [],
      custom_dns: new FormArray([]),
      joinSources: new FormArray([]),
      clustering: new UntypedFormControl(Clustering.Default.charAt(0).toUpperCase() + Clustering.Default.slice(1))
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

  onAuthModeChange() {
    const authMode = this.smbForm.get('auth_mode').value;
    const domainSettingsControl = this.smbForm.get('domain_settings');
    const userGroupSettingsControl = this.smbForm.get('joinSources');

    if (authMode === AuthMode.activeDirectory) {
      const joinSources = this.smbForm.get('joinSources') as FormArray;
      if (joinSources) {
        joinSources.clear();
      }
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
    const domainSettingsControl = this.smbForm.get('domain_settings');
    const authMode = this.smbForm.get('auth_mode').value;
    if (authMode === AuthMode.activeDirectory && !domainSettingsControl.value) {
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
        call: this.smbService.create(requestModel)
      })
      .subscribe({
        complete: () => {
          this.router.navigate([`cephfs/smb`]);
        },
        error() {
          component.smbForm.setErrors({ cdSubmitButton: true });
        },
      });
  }

  private buildRequest() {
    const values = this.smbForm.getRawValue();
    const rawFormValue = _.cloneDeep(this.smbForm.value);
    const joinSources = (this.domainSettingsObject?.joinSources || [])
      .filter((source: any) => source.ref)
      .map((source: any) => ({ ref: source.ref }));

    const domainSettings =
      this.domainSettingsObject?.realm || joinSources.length > 0
        ? {
            realm: this.domainSettingsObject?.realm,
            join_sources:
              joinSources.length > 0
                ? joinSources.map((source: any) => ({
                    source_type: Resources.Resource,
                    ref: source.ref
                  }))
                : []
          }
        : undefined;

    const requestModel: any = {
      cluster_resource: {
        resource_type: Resource_Type,
        cluster_id: rawFormValue.cluster_id,
        auth_mode: rawFormValue.auth_mode
      }
    };

    if (domainSettings) {
      requestModel.cluster_resource.domain_settings = domainSettings;
    }
    if (rawFormValue.joinSources?.length > 0) {
      requestModel.cluster_resource.user_group_settings = rawFormValue.joinSources.map(
        (source: any) => ({
          source_type: Resources.Resource,
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

    if (rawFormValue.clustering && rawFormValue.clustering.toLowerCase() !== Clustering.Default) {
      requestModel.cluster_resource.clustering = rawFormValue.clustering.toLowerCase();
    }

    return requestModel;
  }

  getPlacementSpec(values: any) {
    console.log(values, "val")
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
    this.domainSettingsObject = {};
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

import { Component, OnInit } from '@angular/core';
import { Router } from '@angular/router';
import { forkJoin, Observable } from 'rxjs';
import { map } from 'rxjs/operators';

import _ from 'lodash';
import { ChangeDetectorRef } from '@angular/core';
import { Resource_Type } from '../smb.model';
import { ActionLabelsI18n, URLVerbs } from '~/app/shared/constants/app.constants';
import { Icons } from '~/app/shared/enum/icons.enum';

import { FormArray, FormControl, FormGroup, UntypedFormControl, Validators } from '@angular/forms';
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
  publicAddrPattern = /^(\d{1,3}\.){3}\d{1,3}\/\d{1,2}(\%[a-zA-Z0-9-]+)?$/;
  allClustering: any = [];
  selectedLabels: string[] = [];
  selectedHosts: string[] = [];
  action: any;
  resource: string;
  icons = Icons;
  domainSettingObject: any;
  modalData$ = this.smbService.modalData$;

  constructor(
    private hostService: HostService,
    private formBuilder: CdFormBuilder,
    private cd: ChangeDetectorRef,
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
      this.domainSettingObject = data;
      this.smbForm.get('domain_setting').setValue(data?.realm);
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
    this.allClustering = ['Default', 'Always', 'Never'];
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
      intent: [
        '',
        {
          validators: [Validators.required]
        }
      ],
      domain_setting: [''],
      user_group_setting: [[]],
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
      jointSources: new FormArray([]),
      clustering: new UntypedFormControl('Always')
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
    const domainSettingControl = this.smbForm.get('domain_setting');
    const defineUserPassControl = this.smbForm.get('user_group_setting');

    if (authMode === 'active-directory') {
      domainSettingControl.setValidators(Validators.required);
      defineUserPassControl.clearValidators();
    } else if (authMode === 'user') {
      defineUserPassControl.setValidators(Validators.required);
      domainSettingControl.clearValidators();
    }
    domainSettingControl.updateValueAndValidity();
    defineUserPassControl.updateValueAndValidity();
  }

  submitAction() {
    const requestModel = this.buildRequest();
    let values = this.smbForm.getRawValue();
    const serviceSpec: object = {
      placement: {}
      //  unmanaged: values['unmanaged']
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

    const BASE_URL = 'cephfs/smb/cluster';
    const cluster_id = this.smbForm.get('cluster_id').value;
    const taskUrl = `${BASE_URL}/${URLVerbs.CREATE}`;
    this.taskWrapperService
      .wrapTaskAroundCall({
        task: new FinishedTask(taskUrl, { cluster_id }),
        call: this.smbService.create(requestModel)
      })
      .subscribe({
        error() {
          // self.form.setErrors({ cdSubmitButton: true });
        },
        complete: () => {
          this.router.navigate([`cephfs/smb`]);
        }
      });
  }

  private buildRequest() {
    const rawFormValue = _.cloneDeep(this.smbForm.value);

    const joinSources =
      this.domainSettingObject?.jointSources
        ?.filter((source: any) => source.source_type && source.ref)
        .map((source: any) => ({
          source_type: source.source_type,
          ref: source.ref
        })) || [];

    const requestModel: any = {
      cluster_resource: {
        resource_type: Resource_Type,
        cluster_id: rawFormValue.cluster_id,
        auth_mode: rawFormValue.auth_mode,
        domain_settings: {
          realm: this.domainSettingObject?.realm,
          join_sources: joinSources
        }
      }
    };

    if (rawFormValue.user_group_setting?.length > 0) {
      requestModel.cluster_resource.user_group_setting = rawFormValue.user_group_setting;
    }

    if (rawFormValue.placement && Object.keys(rawFormValue.placement).length > 0) {
      requestModel.cluster_resource.placement = rawFormValue.placement;
    }

    if (rawFormValue.custom_dns?.length > 0) {
      requestModel.cluster_resource.custom_dns = rawFormValue.custom_dns;
    }

    if (rawFormValue.clustering && rawFormValue.clustering.trim() !== '') {
      requestModel.cluster_resource.clustering = rawFormValue.clustering;
    }

    return requestModel;
  }

  showDomainSettingModal() {
    this.modalService.show(SmbDomainSettingModalComponent);
  }

  get jointSources() {
    return this.smbForm.get('jointSources') as FormArray;
  }

  get custom_dns() {
    return this.smbForm.get('custom_dns') as FormArray;
  }

  addUserGroupSetting() {
    this.jointSources.push(
      new FormGroup({
        source_type: new FormControl(''),
        ref: new FormControl('', Validators.required)
      })
    );
    this.cd.detectChanges();
  }

  addCustomDns() {
    const control = new FormControl('', Validators.required);
    this.custom_dns.push(control);
  }

  removeUserGroupSetting(index: number) {
    this.jointSources.removeAt(index);
    this.jointSources.controls.forEach((x) => x.get('ref').updateValueAndValidity());
    this.cd.detectChanges();
  }

  removeCustomDNS(index: number) {
    this.custom_dns.removeAt(index);
  }
}

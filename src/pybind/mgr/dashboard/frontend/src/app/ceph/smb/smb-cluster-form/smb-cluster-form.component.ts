import { Component, OnInit } from '@angular/core';
import { FormControl, UntypedFormControl, Validators } from '@angular/forms';
import _ from 'cypress/types/lodash';

import { forkJoin, Observable } from 'rxjs';
import { map } from 'rxjs/operators';
import { HostService } from '~/app/shared/api/host.service';
import { OrchestratorService } from '~/app/shared/api/orchestrator.service';
import { SmbService } from '~/app/shared/api/smb.service';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { CdForm } from '~/app/shared/forms/cd-form';
import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { SmbDomainSettingModalComponent } from '../smb-domain-setting-modal/smb-domain-setting-modal.component';
import { Icons } from '~/app/shared/enum/icons.enum';

@Component({
  selector: 'cd-smb-cluster-form',
  templateUrl: './smb-cluster-form.component.html',
  styleUrls: ['./smb-cluster-form.component.scss']
})
export class SmbClusterFormComponent extends CdForm implements OnInit{
  
  smbForm: CdFormGroup;
  allClusters: { cluster_id: string }[] = null;
  data:any;
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
  
  constructor( private hostService: HostService, private formBuilder: CdFormBuilder,
    public smbService: SmbService,  public actionLabels: ActionLabelsI18n, 
      private orchService: OrchestratorService,    private modalService: ModalCdsService,) {
super();
this.resource = $localize`Cluster`;
  }
  ngOnInit() {
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
    console.log(this.hostsAndLabels$);


    console.log(this.smbForm.controls.placement.value );
}


  createForm() {
    this.smbForm = this.formBuilder.group({
      resource_type: new FormControl('', {
        validators: [Validators.required]
      }),
      cluster_id: new FormControl('', {
        validators: [Validators.required]
      }),
      auth_mode: ['', {
        validators: [Validators.required]
      }],
      intent: ['', {
        validators: [Validators.required]
      }],
      domain_setting: [''],
      user_group_setting: [''],
      placement: [''],
      hosts: [[]],
      label: [
        null,
        [
          CdValidators.requiredIf({
            placement: 'label',
          })
        ]
      ],
      custom_dns: [[]],
      public_addrs: ['', Validators.pattern(this.publicAddrPattern)],
      clustering: new UntypedFormControl('Always')
    })

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
    const defineUserPassControl = this.smbForm.get('user_group_setting')

    if (authMode === 'active-directory') {
      domainSettingControl.setValidators(Validators.required);
      defineUserPassControl.clearValidators();
    }
    else if(authMode === 'user'){
      defineUserPassControl.setValidators(Validators.required);
      domainSettingControl.clearValidators();
    }
    domainSettingControl.updateValueAndValidity();
    defineUserPassControl.updateValueAndValidity();
  }

  submitAction() {
  
    console.log(this.smbForm, "lihkug");
    const values = this.smbForm.getRawValue();
      const serviceSpec: object = {
        placement: {},
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

    
   
      if (this.action === this.actionLabels.CREATE) {
      
        this.smbService.create(values).subscribe(
          () => {
            // this.notificationService.show(
            //   NotificationType.success,
            //   $localize`SMB Cluster: '${values['cluster_id']}' created successfully`
            // );
           
          },
          () => {
          //  this.multisiteRealmForm.setErrors({ cdSubmitButton: true });
          }
        );
      }
    }

    showDomainSettingModal() {
 this.modalService.show(SmbDomainSettingModalComponent);

   console.log("test");
  }
}

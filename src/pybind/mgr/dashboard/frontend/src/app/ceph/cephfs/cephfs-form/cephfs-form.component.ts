import { Component, OnInit, ViewChild } from '@angular/core';
import { FormControl, Validators } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';
import _ from 'lodash';

import { NgbNav, NgbTooltip, NgbTypeahead } from '@ng-bootstrap/ng-bootstrap';
import { forkJoin, Observable, Subject } from 'rxjs';
import { map } from 'rxjs/operators';

import { CephfsService } from '~/app/shared/api/cephfs.service';
import { HostService } from '~/app/shared/api/host.service';
import { OrchestratorService } from '~/app/shared/api/orchestrator.service';
import { ActionLabelsI18n, URLVerbs } from '~/app/shared/constants/app.constants';
import { Icons } from '~/app/shared/enum/icons.enum';
import { CdForm } from '~/app/shared/forms/cd-form';
import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { Permission } from '~/app/shared/models/permissions';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';

@Component({
  selector: 'cd-cephfs-form',
  templateUrl: './cephfs-form.component.html',
  styleUrls: ['./cephfs-form.component.scss']
})
export class CephfsVolumeFormComponent extends CdForm implements OnInit {
  @ViewChild('crushInfoTabs') crushInfoTabs: NgbNav;
  @ViewChild('crushDeletionBtn') crushDeletionBtn: NgbTooltip;
  @ViewChild('ecpInfoTabs') ecpInfoTabs: NgbNav;
  @ViewChild('ecpDeletionBtn') ecpDeletionBtn: NgbTooltip;
  @ViewChild(NgbTypeahead, { static: false })
  typeahead: NgbTypeahead;

  labelFocus = new Subject<string>();
  labelClick = new Subject<string>();

  orchStatus$: Observable<any>;

  permission: Permission;
  form: CdFormGroup;
  action: string;
  resource: string;
  editing: boolean;
  icons = Icons;
  hosts: any;
  labels: any;
  hasOrchestrator: boolean;
  currentVolumeName: string;
  fsId: number;
  disableRename: boolean = true;
  hostsAndLabels$: Observable<{ hosts: any[]; labels: any[] }>;

  fsFailCmd: string;
  fsSetCmd: string;

  selectedLabels: string[] = [];
  selectedHosts: string[] = [];

  constructor(
    private router: Router,
    private taskWrapperService: TaskWrapperService,
    private orchService: OrchestratorService,
    private formBuilder: CdFormBuilder,
    public actionLabels: ActionLabelsI18n,
    private hostService: HostService,
    private cephfsService: CephfsService,
    private route: ActivatedRoute
  ) {
    super();
    this.editing = this.router.url.startsWith(`/cephfs/fs/${URLVerbs.EDIT}`);
    this.action = this.editing ? this.actionLabels.EDIT : this.actionLabels.CREATE;
    this.resource = $localize`File System`;
    this.createForm();
  }

  private createForm() {
    this.form = this.formBuilder.group({
      name: new FormControl('', {
        validators: [
          Validators.pattern(/^(?:[.][A-Za-z0-9_-]+|[A-Za-z][.A-Za-z0-9_-]*)$/),
          Validators.required
        ]
      }),
      placement: [''],
      hosts: [[]],
      label: [
        null,
        [
          CdValidators.requiredIf({
            placement: 'label',
            unmanaged: false
          })
        ]
      ],
      unmanaged: [false]
    });
    this.orchService.status().subscribe((status) => {
      this.hasOrchestrator = status.available;
      this.form.get('placement').setValue(this.hasOrchestrator ? 'hosts' : '');
    });
  }

  ngOnInit() {
    if (this.editing) {
      this.route.params.subscribe((params: { id: string }) => {
        this.fsId = Number(params.id);
      });

      this.cephfsService.getCephfs(this.fsId).subscribe((resp: object) => {
        this.currentVolumeName = resp['cephfs']['name'];
        this.form.get('name').setValue(this.currentVolumeName);

        this.disableRename = !(
          !resp['cephfs']['flags']['joinable'] && resp['cephfs']['flags']['refuse_client_session']
        );
        if (this.disableRename) {
          this.form.get('name').disable();
          this.fsFailCmd = `ceph fs fail ${this.currentVolumeName}`;
          this.fsSetCmd = `ceph fs set ${this.currentVolumeName} refuse_client_session true`;
        }
      });
    } else {
      this.hostsAndLabels$ = forkJoin({
        hosts: this.hostService.getAllHosts(),
        labels: this.hostService.getLabels()
      }).pipe(
        map(({ hosts, labels }) => ({
          hosts: hosts.map((host: any) => ({ content: host['hostname'] })),
          labels: labels.map((label: string) => ({ content: label }))
        }))
      );
    }
    this.orchStatus$ = this.orchService.status();
    this.loadingReady();
  }

  multiSelector(event: any, field: 'label' | 'hosts') {
    if (field === 'label') this.selectedLabels = event.map((label: any) => label.content);
    else this.selectedHosts = event.map((host: any) => host.content);
  }

  submit() {
    const volumeName = this.form.get('name').value;
    const BASE_URL = 'cephfs';

    if (this.editing) {
      this.taskWrapperService
        .wrapTaskAroundCall({
          task: new FinishedTask(`${BASE_URL}/${URLVerbs.EDIT}`, {
            volumeName: volumeName
          }),
          call: this.cephfsService.rename(this.currentVolumeName, volumeName)
        })
        .subscribe({
          error: () => {
            this.form.setErrors({ cdSubmitButton: true });
          },
          complete: () => {
            this.router.navigate([`${BASE_URL}/fs`]);
          }
        });
    } else {
      let values = this.form.getRawValue();
      const serviceSpec: object = {
        placement: {},
        unmanaged: values['unmanaged']
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

      const self = this;
      let taskUrl = `${BASE_URL}/${URLVerbs.CREATE}`;
      this.taskWrapperService
        .wrapTaskAroundCall({
          task: new FinishedTask(taskUrl, {
            volumeName: volumeName
          }),
          call: this.cephfsService.create(this.form.get('name').value, serviceSpec)
        })
        .subscribe({
          error() {
            self.form.setErrors({ cdSubmitButton: true });
          },
          complete: () => {
            this.router.navigate([`${BASE_URL}/fs`]);
          }
        });
    }
  }
}

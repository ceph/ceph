import { Component, OnInit } from '@angular/core';
import { UntypedFormControl, Validators } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { ListenerRequest, NvmeofService } from '~/app/shared/api/nvmeof.service';
import { ActionLabelsI18n, URLVerbs } from '~/app/shared/constants/app.constants';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { Permission } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { FormatterService } from '~/app/shared/services/formatter.service';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { DimlessBinaryPipe } from '~/app/shared/pipes/dimless-binary.pipe';
import { HostService } from '~/app/shared/api/host.service';
import { map } from 'rxjs/operators';
import { forkJoin } from 'rxjs';
import { CephServiceSpec } from '~/app/shared/models/service.interface';

@Component({
  selector: 'cd-nvmeof-listeners-form',
  templateUrl: './nvmeof-listeners-form.component.html',
  styleUrls: ['./nvmeof-listeners-form.component.scss']
})
export class NvmeofListenersFormComponent implements OnInit {
  action: string;
  permission: Permission;
  hostPermission: Permission;
  resource: string;
  pageURL: string;
  listenerForm: CdFormGroup;
  subsystemNQN: string;
  hosts: Array<object> = null;
  group: string;

  constructor(
    public actionLabels: ActionLabelsI18n,
    private authStorageService: AuthStorageService,
    private taskWrapperService: TaskWrapperService,
    private nvmeofService: NvmeofService,
    private hostService: HostService,
    private router: Router,
    private route: ActivatedRoute,
    public activeModal: NgbActiveModal,
    public formatterService: FormatterService,
    public dimlessBinaryPipe: DimlessBinaryPipe
  ) {
    this.permission = this.authStorageService.getPermissions().nvmeof;
    this.hostPermission = this.authStorageService.getPermissions().hosts;
    this.resource = $localize`Listener`;
    this.pageURL = 'block/nvmeof/subsystems';
  }

  setHosts() {
    forkJoin({
      gwGroups: this.nvmeofService.listGatewayGroups(),
      hosts: this.hostService.getAllHosts()
    })
      .pipe(
        map(({ gwGroups, hosts }) => {
          // Find the gateway hosts in current group
          const selectedGwGroup: CephServiceSpec = gwGroups?.[0]?.find(
            (gwGroup: CephServiceSpec) => gwGroup?.spec?.group === this.group
          );
          const gatewayHosts: string[] = selectedGwGroup?.placement?.hosts;
          // Return the gateway hosts in current group with their metadata
          return gatewayHosts
            ? hosts.filter((host: any) => gatewayHosts.includes(host.hostname))
            : [];
        })
      )
      .subscribe((nvmeofHosts: any[]) => {
        this.hosts = nvmeofHosts.map((h) => ({ hostname: h.hostname, addr: h.addr }));
      });
  }

  ngOnInit() {
    this.createForm();
    this.action = this.actionLabels.CREATE;
    this.route.params.subscribe((params: { subsystem_nqn: string }) => {
      this.subsystemNQN = params?.subsystem_nqn;
    });
    this.route.queryParams.subscribe((params) => {
      this.group = params?.['group'];
    });
    this.setHosts();
  }

  createForm() {
    this.listenerForm = new CdFormGroup({
      host: new UntypedFormControl(null, {
        validators: [Validators.required]
      }),
      trsvcid: new UntypedFormControl(4420, [
        Validators.required,
        CdValidators.number(false),
        Validators.max(65535)
      ])
    });
  }

  buildRequest(): ListenerRequest {
    const host = this.listenerForm.getValue('host');
    let trsvcid = Number(this.listenerForm.getValue('trsvcid'));
    if (!trsvcid) trsvcid = 4420;
    const request: ListenerRequest = {
      gw_group: this.group,
      host_name: host.hostname,
      traddr: host.addr,
      trsvcid
    };
    return request;
  }

  onSubmit() {
    const component = this;
    const taskUrl: string = `nvmeof/listener/${URLVerbs.CREATE}`;
    const request = this.buildRequest();
    this.taskWrapperService
      .wrapTaskAroundCall({
        task: new FinishedTask(taskUrl, {
          nqn: this.subsystemNQN,
          host_name: request.host_name
        }),
        call: this.nvmeofService.createListener(this.subsystemNQN, request)
      })
      .subscribe({
        error() {
          component.listenerForm.setErrors({ cdSubmitButton: true });
        },
        complete: () => {
          this.router.navigate([this.pageURL, { outlets: { modal: null } }]);
        }
      });
  }
}

import { Component, Inject, OnInit, Optional, ViewChild } from '@angular/core';
import { UntypedFormControl } from '@angular/forms';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { BaseModal } from 'carbon-components-angular';
import { CephUserService } from '~/app/shared/api/ceph-user.service';
import { MonitorService } from '~/app/shared/api/monitor.service';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';

@Component({
  selector: 'cd-cephfs-mount-details',
  templateUrl: './cephfs-mount-details.component.html',
  styleUrls: ['./cephfs-mount-details.component.scss']
})
export class CephfsMountDetailsComponent extends BaseModal implements OnInit {
  @ViewChild('mountDetailsTpl', { static: true })
  mountDetailsTpl: any;
  mountDetail!: Record<string, any>;
  onCancel?: Function;
  private MOUNT_DIRECTORY = '<MOUNT_DIRECTORY>';
  filteredUsers: any[] = [];
  selectedUser: string = 'client.admin';
  updatedUser: any;
  publicaddr: any;
  secret: any;
  userForm: CdFormGroup;
  mount!: string;
  cephfsmount!: string;
  fuse!: string;
  nfs!: string;
  extractedKey!: string;

  constructor(
    public activeModal: NgbActiveModal,
    private monitorService: MonitorService,
    public cephUserService: CephUserService,
    @Optional() @Inject('mountData') public mountData: object[]
  ) {
    super();
  }

  ngOnInit(): void {
    this.mountDetail = this.mountData;
    this.userForm = new CdFormGroup({
      user: new UntypedFormControl(this.selectedUser, {})
    });

    this.getUser();
    this.userChangeHandler();
    this.mount = `sudo mount -t ceph <CLIENT_USER>@${this.mountDetail?.fsId}.${this.mountDetail?.fsName}=${this.mountDetail?.rootPath} ${this.MOUNT_DIRECTORY}`;
    this.fuse = `sudo ceph-fuse  ${this.MOUNT_DIRECTORY} -r ${this.mountDetail?.rootPath} --client_mds_namespace=${this.mountDetail?.fsName}`;
    this.nfs = `sudo mount -t nfs -o port=<PORT> <IP of active_nfs daemon>:<export_name> ${this.MOUNT_DIRECTORY}`;
  }

  cancel() {
    this.closeModal();
  }

  getUser() {
    this.cephUserService.getUser().subscribe((data: any) => {
      this.filteredUsers = data.filter((data: { entity: string }) =>
        data.entity.startsWith('client.')
      );
    });
  }

  userChangeHandler() {
    this.selectedUser = this.userForm.getValue('user');
    this.updatedUser = this.selectedUser.replace('client.', '');
    this.getKey(this.selectedUser);
    this.getMonitorData();
  }

  getMonitorData() {
    this.monitorService.getMonitor().subscribe((data: any) => {
      this.publicaddr = data.in_quorum
        .map((item: { public_addr: string }) => item.public_addr.split('/')[0])
        .join(',');
      this.cephfsmount = `sudo mount -t ceph ${this.publicaddr}:/ /${this.MOUNT_DIRECTORY} -o name=${this.updatedUser},fs=${this.mountDetail.fsName},secret=${this.extractedKey}`;
    });
  }

  getKey(entities: any) {
    const entitiesArray = [entities];
    this.cephUserService.export(entitiesArray).subscribe((data: string) => {
      const keyMatch = data.match(/key\s*=\s*([^\s]+)/);
      this.extractedKey = keyMatch[1];
    });
  }

}

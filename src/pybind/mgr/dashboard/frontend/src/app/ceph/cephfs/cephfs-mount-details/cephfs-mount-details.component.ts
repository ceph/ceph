import { Component, Inject, OnInit, Optional, ViewChild } from '@angular/core';
import { BaseModal } from 'carbon-components-angular';
import { MountData } from '~/app/shared/models/cephfs.model';
import { UntypedFormControl } from '@angular/forms';
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
  filteredUsers: string[] = [];
  selectedUser: string = 'client.admin';
  updatedUser: string;
  publicaddr: string;
  secret: string;
  userForm: CdFormGroup;
  mount!: string;
  cephfsmount!: string;
  fuse!: string;
  nfs!: string;
  extractedKey!: string;
  uniqueIp: string[];

  constructor(
    private monitorService: MonitorService,
    public cephUserService: CephUserService,
    @Optional() @Inject('mountData') public mountData: MountData,
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
  this.monitorService.getMonitor().subscribe({
    next: (data: any) => {
      this.publicaddr = data.in_quorum
        .map((item: { public_addr: string }) => item.public_addr.split('/')[0])
        .join(',');

      this.uniqueIp = [...new Set(this.publicaddr.match(/\b(\d+\.\d+\.\d+\.\d+)(?=:)/g))];

      const path = this.mountDetail?.path ?? '<PATH>';
      const fsName = this.mountDetail?.fsName ?? '<FS_NAME>';

      this.cephfsmount = `sudo mount -t ceph ${this.publicaddr}:/ /${this.MOUNT_DIRECTORY} -o name=${this.updatedUser},fs=${fsName},secret=${this.extractedKey}`;
      this.fuse = `sudo ceph-fuse ${this.MOUNT_DIRECTORY} -r ${path} --client_mds_namespace=${fsName}`;
    },
    error: () => {
      const path = this.mountDetail?.path ?? '<PATH>';
      const fsName = this.mountDetail?.fsName ?? '<FS_NAME>';
      const clusterFSID = this.mountDetail?.clusterFSID ?? '<CLUSTER_ID>';

      this.cephfsmount = `sudo mount -t ceph <CLIENT_USER>@${clusterFSID}.${fsName}=${path} ${this.MOUNT_DIRECTORY}`;
      this.fuse = `sudo ceph-fuse ${this.MOUNT_DIRECTORY} -r ${path} --client_mds_namespace=${fsName}`;
    }
  });
}

  getKey(entities: any) {
    const entitiesArray = [entities];
    this.cephUserService.export(entitiesArray).subscribe((data: string) => {
      const keyMatch = data.match(/key\s*=\s*([^\s]+)/);

      if (keyMatch && keyMatch[1]) {
        this.extractedKey = keyMatch[1];
      } else {
        this.extractedKey = '<SECRET_KEY>';
      }
    });
  }
}

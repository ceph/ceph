import { Component, Inject, OnInit, Optional, ViewChild } from '@angular/core';
import { BaseModal } from 'carbon-components-angular';
import { MountData } from '~/app/shared/models/cephfs.model';
import { UntypedFormControl } from '@angular/forms';
import { CephUserService } from '~/app/shared/api/ceph-user.service';
import { MonitorService } from '~/app/shared/api/monitor.service';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import {
  IPV4EXTRACTIONREGEX,
  IPV6EXTRACTIONREGEX
} from '~/app/shared/constants/regex-patterns.constants';
import { CephUser, MonitorData } from '~/app/shared/models/client-user.model';

@Component({
  selector: 'cd-cephfs-mount-details',
  templateUrl: './cephfs-mount-details.component.html',
  styleUrls: ['./cephfs-mount-details.component.scss'],
  standalone: false
})
export class CephfsMountDetailsComponent extends BaseModal implements OnInit {
  @ViewChild('mountDetailsTpl', { static: true })
  mountDetailsTpl: any;
  mountDetail!: Record<string, any>;
  onCancel?: Function;
  private MOUNT_DIRECTORY = '<MOUNT_DIRECTORY>';
  private SECRETKEY = '<SECRET_KEY>';
  private PATH = '<PATH>';
  private FSNAME = '<FS_NAME>';
  private CLUSTERID = '<CLUSTER_ID>';
  private CLIENTUSER = '<CLIENT_USER>';
  private CLIENTPREFIX = 'client.';
  clients: CephUser[] = [];
  selectedUser: string = `${this.CLIENTPREFIX}admin`;
  updatedUser: string;
  publicaddr: string;
  userForm: CdFormGroup;
  cephfsmount!: string;
  fuse!: string;
  extractedKey!: string;
  uniqueIp: string[];

  constructor(
    private monitorService: MonitorService,
    public cephUserService: CephUserService,
    @Optional() @Inject('mountData') public mountData: MountData
  ) {
    super();
  }

  ngOnInit(): void {
    this.mountDetail = this.mountData;
    this.userForm = new CdFormGroup({
      user: new UntypedFormControl(this.selectedUser, {})
    });
    this.getUser();
  }

  cancel() {
    this.closeModal();
  }

  getUser() {
    this.cephUserService.getCephUser().subscribe((cephUsers: CephUser[]) => {
      this.clients = cephUsers
        .filter((user: CephUser) => user.entity.startsWith(this.CLIENTPREFIX))
        .map((user: CephUser) => ({
          entity: user.entity,
          key: user.key
        }));
      this.userChangeHandler();
    });
  }

  userChangeHandler() {
    this.selectedUser = this.userForm.getValue('user');
    this.updatedUser = this.selectedUser.replace(this.CLIENTPREFIX, '');

    const user = this.clients.find((user: CephUser) => user.entity === this.selectedUser);
    if (user) {
      this.extractedKey = user.key;
    } else {
      this.extractedKey = this.SECRETKEY;
    }
    this.getMonitorData();
  }

  getMonitorData() {
    this.monitorService.getMonitor().subscribe({
      next: (monitorData: MonitorData) => {
        const publicAddrs = monitorData.in_quorum.map((item: { public_addr: string }) =>
          item.public_addr.split('/')[0].trim()
        );

        const address = publicAddrs.map((addr) => {
          addr = addr.replace(/^https?:\/\//, '');

          const ipv6Match = addr.match(IPV6EXTRACTIONREGEX);
          if (ipv6Match) return ipv6Match[1];

          const ipv4Match = addr.match(IPV4EXTRACTIONREGEX);
          if (ipv4Match) return ipv4Match[1];

          return addr.split(':')[0];
        });

        this.publicaddr = address.join(',');

        this.uniqueIp = [...new Set(address)];

        const fsName = this.mountDetail?.fsName ?? this.FSNAME;

        const keyring = `/etc/ceph/ceph.${this.selectedUser}.keyring`;

        this.cephfsmount = this.getCephFSMount(fsName, keyring);
        this.fuse = this.getFuse();
      },
      error: () => {
        const path = this.mountDetail?.path ?? this.PATH;
        const fsName = this.mountDetail?.fsName ?? this.FSNAME;
        const clusterFSID = this.mountDetail?.clusterFSID ?? this.CLUSTERID;

        this.cephfsmount = `sudo mount -t ceph ${this.CLIENTUSER}@${clusterFSID}.${fsName}=${path} ${this.MOUNT_DIRECTORY}`;
        this.fuse = `sudo ceph-fuse ${this.MOUNT_DIRECTORY} -r ${path} --client_mds_namespace=${fsName}`;
      }
    });
  }

  getCephFSMount(fsName: string, keyring: string): string {
    return `sudo mount -t ceph ${this.publicaddr}:/ ${this.MOUNT_DIRECTORY} -o name=${this.updatedUser},fs=${fsName},keyring=${keyring}`;
  }

  getFuse(): string {
    return `sudo ceph-fuse -n ${this.selectedUser} -m ${this.publicaddr} ${this.MOUNT_DIRECTORY} --key=${this.extractedKey}`;
  }
}

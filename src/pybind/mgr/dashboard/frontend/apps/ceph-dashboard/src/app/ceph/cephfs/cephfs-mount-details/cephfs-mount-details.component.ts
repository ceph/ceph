import { Component, OnInit, ViewChild } from '@angular/core';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { BaseModal } from 'carbon-components-angular';

@Component({
  selector: 'cd-cephfs-mount-details',
  templateUrl: './cephfs-mount-details.component.html',
  styleUrls: ['./cephfs-mount-details.component.scss']
})
export class CephfsMountDetailsComponent extends BaseModal implements OnInit {
  @ViewChild('mountDetailsTpl', { static: true })
  mountDetailsTpl: any;
  onCancel?: Function;
  private MOUNT_DIRECTORY = '<MOUNT_DIRECTORY>';
  mountData!: Record<string, any>;
  constructor(public activeModal: NgbActiveModal) {
    super();
  }
  mount!: string;
  fuse!: string;
  nfs!: string;

  ngOnInit(): void {
    this.mount = `sudo mount -t ceph <CLIENT_USER>@${this.mountData?.fsId}.${this.mountData?.fsName}=${this.mountData?.rootPath} ${this.MOUNT_DIRECTORY}`;
    this.fuse = `sudo ceph-fuse  ${this.MOUNT_DIRECTORY} -r ${this.mountData?.rootPath} --client_mds_namespace=${this.mountData?.fsName}`;
    this.nfs = `sudo mount -t nfs -o port=<PORT> <IP of active_nfs daemon>:<export_name> ${this.MOUNT_DIRECTORY}`;
  }

  cancel() {
    this.closeModal();
  }
}

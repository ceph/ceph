import { Component, Input, OnChanges } from '@angular/core';

import { CdTableColumn } from '../../../shared/models/cd-table-column';

@Component({
  selector: 'cd-nfs-details',
  templateUrl: './nfs-details.component.html',
  styleUrls: ['./nfs-details.component.scss']
})
export class NfsDetailsComponent implements OnChanges {
  @Input()
  selection: any;

  selectedItem: any;
  data: any;

  clientsColumns: CdTableColumn[];
  clients: any[] = [];

  constructor() {
    this.clientsColumns = [
      {
        name: $localize`Addresses`,
        prop: 'addresses',
        flexGrow: 2
      },
      {
        name: $localize`Access Type`,
        prop: 'access_type',
        flexGrow: 1
      },
      {
        name: $localize`Squash`,
        prop: 'squash',
        flexGrow: 1
      }
    ];
  }

  ngOnChanges() {
    if (this.selection) {
      this.selectedItem = this.selection;

      this.clients = this.selectedItem.clients;

      this.data = {};
      this.data[$localize`Cluster`] = this.selectedItem.cluster_id;
      this.data[$localize`Daemons`] = this.selectedItem.daemons;
      this.data[$localize`NFS Protocol`] = this.selectedItem.protocols.map(
        (protocol: string) => 'NFSv' + protocol
      );
      this.data[$localize`Pseudo`] = this.selectedItem.pseudo;
      this.data[$localize`Access Type`] = this.selectedItem.access_type;
      this.data[$localize`Squash`] = this.selectedItem.squash;
      this.data[$localize`Transport`] = this.selectedItem.transports;
      this.data[$localize`Path`] = this.selectedItem.path;

      if (this.selectedItem.fsal.name === 'CEPH') {
        this.data[$localize`Storage Backend`] = $localize`CephFS`;
        this.data[$localize`CephFS User`] = this.selectedItem.fsal.user_id;
        this.data[$localize`CephFS Filesystem`] = this.selectedItem.fsal.fs_name;
        this.data[$localize`Security Label`] = this.selectedItem.fsal.sec_label_xattr;
      } else {
        this.data[$localize`Storage Backend`] = $localize`Object Gateway`;
        this.data[$localize`Object Gateway User`] = this.selectedItem.fsal.rgw_user_id;
      }
    }
  }
}

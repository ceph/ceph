import { Component, Input, OnChanges } from '@angular/core';

import { I18n } from '@ngx-translate/i18n-polyfill';

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

  constructor(private i18n: I18n) {
    this.clientsColumns = [
      {
        name: this.i18n('Addresses'),
        prop: 'addresses',
        flexGrow: 2
      },
      {
        name: this.i18n('Access Type'),
        prop: 'access_type',
        flexGrow: 1
      },
      {
        name: this.i18n('Squash'),
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
      this.data[this.i18n('Cluster')] = this.selectedItem.cluster_id;
      this.data[this.i18n('Daemons')] = this.selectedItem.daemons;
      this.data[this.i18n('NFS Protocol')] = this.selectedItem.protocols.map(
        (protocol: string) => 'NFSv' + protocol
      );
      this.data[this.i18n('Pseudo')] = this.selectedItem.pseudo;
      this.data[this.i18n('Access Type')] = this.selectedItem.access_type;
      this.data[this.i18n('Squash')] = this.selectedItem.squash;
      this.data[this.i18n('Transport')] = this.selectedItem.transports;
      this.data[this.i18n('Path')] = this.selectedItem.path;

      if (this.selectedItem.fsal.name === 'CEPH') {
        this.data[this.i18n('Storage Backend')] = this.i18n('CephFS');
        this.data[this.i18n('CephFS User')] = this.selectedItem.fsal.user_id;
        this.data[this.i18n('CephFS Filesystem')] = this.selectedItem.fsal.fs_name;
        this.data[this.i18n('Security Label')] = this.selectedItem.fsal.sec_label_xattr;
      } else {
        this.data[this.i18n('Storage Backend')] = this.i18n('Object Gateway');
        this.data[this.i18n('Object Gateway User')] = this.selectedItem.fsal.rgw_user_id;
      }
    }
  }
}

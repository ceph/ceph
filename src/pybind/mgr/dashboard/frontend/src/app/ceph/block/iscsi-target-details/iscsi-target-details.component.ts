import { Component, Input, OnChanges } from '@angular/core';

import { NodeEvent, TreeModel } from 'ng2-tree';

import { CdTableSelection } from '../../../shared/models/cd-table-selection';

@Component({
  selector: 'cd-iscsi-target-details',
  templateUrl: './iscsi-target-details.component.html',
  styleUrls: ['./iscsi-target-details.component.scss']
})
export class IscsiTargetDetailsComponent implements OnChanges {
  @Input()
  selection: CdTableSelection;
  selectedItem: any;

  tree: TreeModel;
  metadata: any = {};

  data: any;
  title: string;

  constructor() {}

  ngOnChanges() {
    if (this.selection.hasSelection) {
      this.selectedItem = this.selection.first();
      this.generateTree();
      console.log(this.selectedItem);
    }
  }

  private generateTree() {
    this.metadata = { root: this.selectedItem.target_controls };

    const cssClasses = {
      target: {
        expanded: 'fa fa-fw fa-bullseye fa-lg'
      },
      hosts: {
        expanded: 'fa fa-fw fa-user fa-lg',
        leaf: 'fa fa-fw fa-user'
      },
      hostGroups: {
        expanded: 'fa fa-fw fa-users fa-lg',
        leaf: 'fa fa-fw fa-users'
      },
      disks: {
        expanded: 'fa fa-fw fa-hdd-o fa-lg',
        leaf: 'fa fa-fw fa-hdd-o'
      },
      portals: {
        expanded: 'fa fa-fw fa-server fa-lg',
        leaf: 'fa fa-fw fa-server fa-lg'
      }
    };

    const disks = [];
    this.selectedItem.disks.forEach((disk) => {
      const id = 'disk_' + disk.pool + '_' + disk.image;
      this.metadata[id] = disk.controls;
      disks.push({
        value: `${disk.pool}/${disk.image}`,
        id: id
      });
    });

    const portals = [];
    this.selectedItem.portals.forEach((portal) => {
      portals.push({ value: `${portal.host}:${portal.ip}` });
    });

    const clients = [];
    this.selectedItem.clients.forEach((client) => {
      this.metadata['client_' + client.client_iqn] = client.auth;

      const luns = [];
      client.luns.forEach((lun) => {
        luns.push({
          value: `${lun.pool}/${lun.image}`,
          id: 'disk_' + lun.pool + '_' + lun.image,
          settings: {
            cssClasses: cssClasses.disks
          }
        });
      });

      clients.push({
        value: client.client_iqn,
        id: 'client_' + client.client_iqn,
        children: luns
      });
    });

    const groups = [];
    this.selectedItem.groups.forEach((group) => {
      const luns = [];
      group.disks.forEach((disk) => {
        luns.push({
          value: `${disk.pool}/${disk.image}`,
          id: 'disk_' + disk.pool + '_' + disk.image
        });
      });

      const hosts = [];
      group.members.forEach((member) => {
        hosts.push({
          value: member,
          id: 'client_' + member
        });
      });

      groups.push({
        value: group.group_id,
        children: [
          {
            value: 'Disks',
            children: luns,
            settings: {
              selectionAllowed: false,
              cssClasses: cssClasses.disks
            }
          },
          {
            value: 'Hosts',
            children: hosts,
            settings: {
              selectionAllowed: false,
              cssClasses: cssClasses.hosts
            }
          }
        ]
      });
    });

    this.tree = {
      value: this.selectedItem.target_iqn,
      id: 'root',
      settings: {
        static: true,
        cssClasses: cssClasses.target
      },
      children: [
        {
          value: 'Disks',
          children: disks,
          settings: {
            selectionAllowed: false,
            cssClasses: cssClasses.disks
          }
        },
        {
          value: 'Portals',
          children: portals,
          settings: {
            selectionAllowed: false,
            cssClasses: cssClasses.portals
          }
        },
        {
          value: 'Host Groups',
          children: groups,
          settings: {
            selectionAllowed: false,
            cssClasses: cssClasses.hostGroups
          }
        },
        {
          value: 'Hosts',
          children: clients,
          settings: {
            selectionAllowed: false,
            cssClasses: cssClasses.hosts
          }
        }
      ]
    };
  }

  onNodeSelected(e: NodeEvent) {
    this.title = e.node.value;
    this.data = this.metadata[e.node.id] || [];
  }
}


//TODO renomear hosts to initiators.

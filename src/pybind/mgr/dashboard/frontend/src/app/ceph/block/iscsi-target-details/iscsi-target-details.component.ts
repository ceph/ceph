import { Component, Input, OnChanges, OnInit, TemplateRef, ViewChild } from '@angular/core';

import { I18n } from '@ngx-translate/i18n-polyfill';
import * as _ from 'lodash';
import { NodeEvent, TreeModel } from 'ng2-tree';

import { TableComponent } from '../../../shared/datatable/table/table.component';
import { CdTableColumn } from '../../../shared/models/cd-table-column';
import { CdTableSelection } from '../../../shared/models/cd-table-selection';

@Component({
  selector: 'cd-iscsi-target-details',
  templateUrl: './iscsi-target-details.component.html',
  styleUrls: ['./iscsi-target-details.component.scss']
})
export class IscsiTargetDetailsComponent implements OnChanges, OnInit {
  @Input()
  selection: CdTableSelection;
  @Input()
  settings: any;

  @ViewChild('highlightTpl')
  highlightTpl: TemplateRef<any>;

  private detailTable: TableComponent;
  @ViewChild('detailTable')
  set content(content: TableComponent) {
    this.detailTable = content;
    if (content) {
      content.updateColumns();
    }
  }

  columns: CdTableColumn[];
  data: any;
  metadata: any = {};
  selectedItem: any;
  title: string;
  tree: TreeModel;

  constructor(private i18n: I18n) {}

  ngOnInit() {
    this.columns = [
      {
        prop: 'displayName',
        name: this.i18n('Name'),
        flexGrow: 2,
        cellTemplate: this.highlightTpl
      },
      {
        prop: 'current',
        name: this.i18n('Current'),
        flexGrow: 1,
        cellTemplate: this.highlightTpl
      },
      {
        prop: 'default',
        name: this.i18n('Default'),
        flexGrow: 1,
        cellTemplate: this.highlightTpl
      }
    ];
  }

  ngOnChanges() {
    if (this.selection.hasSelection) {
      this.selectedItem = this.selection.first();
      this.generateTree();
    }

    this.data = undefined;
  }

  private generateTree() {
    this.metadata = { root: this.selectedItem.target_controls };

    const cssClasses = {
      target: {
        expanded: 'fa fa-fw fa-bullseye fa-lg'
      },
      initiators: {
        expanded: 'fa fa-fw fa-user fa-lg',
        leaf: 'fa fa-fw fa-user'
      },
      groups: {
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
    _.forEach(this.selectedItem.disks, (disk) => {
      const id = 'disk_' + disk.pool + '_' + disk.image;
      this.metadata[id] = {
        controls: disk.controls,
        backstore: disk.backstore
      };

      disks.push({
        value: `${disk.pool}/${disk.image}`,
        id: id
      });
    });

    const portals = [];
    _.forEach(this.selectedItem.portals, (portal) => {
      portals.push({ value: `${portal.host}:${portal.ip}` });
    });

    const clients = [];
    _.forEach(this.selectedItem.clients, (client) => {
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
    _.forEach(this.selectedItem.groups, (group) => {
      const luns = [];
      group.disks.forEach((disk) => {
        luns.push({
          value: `${disk.pool}/${disk.image}`,
          id: 'disk_' + disk.pool + '_' + disk.image
        });
      });

      const initiators = [];
      group.members.forEach((member) => {
        initiators.push({
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
            value: 'Initiators',
            children: initiators,
            settings: {
              selectionAllowed: false,
              cssClasses: cssClasses.initiators
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
          value: 'Initiators',
          children: clients,
          settings: {
            selectionAllowed: false,
            cssClasses: cssClasses.initiators
          }
        },
        {
          value: 'Groups',
          children: groups,
          settings: {
            selectionAllowed: false,
            cssClasses: cssClasses.groups
          }
        }
      ]
    };
  }

  onNodeSelected(e: NodeEvent) {
    if (e.node.id) {
      this.title = e.node.value;
      const tempData = this.metadata[e.node.id] || {};

      if (e.node.id === 'root') {
        this.columns[2].isHidden = false;
        this.data = _.map(this.settings.target_default_controls, (value, key) => {
          return {
            displayName: key,
            default: value,
            current: tempData[key] || value
          };
        });
      } else if (e.node.id.toString().startsWith('disk_')) {
        this.columns[2].isHidden = false;
        this.data = _.map(this.settings.disk_default_controls[tempData.backstore], (value, key) => {
          return {
            displayName: key,
            default: value,
            current: !_.isUndefined(tempData.controls[key]) ? tempData.controls[key] : value
          };
        });
        this.data.push({
          displayName: 'backstore',
          default: this.settings.default_backstore,
          current: tempData.backstore
        });
      } else {
        this.columns[2].isHidden = true;
        this.data = _.map(tempData, (value, key) => {
          return {
            displayName: key,
            default: undefined,
            current: value
          };
        });
      }
    } else {
      this.data = undefined;
    }

    if (this.detailTable) {
      this.detailTable.updateColumns();
    }
  }
}

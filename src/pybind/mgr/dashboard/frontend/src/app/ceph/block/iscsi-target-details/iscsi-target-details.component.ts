import { Component, Input, OnChanges, OnInit, TemplateRef, ViewChild } from '@angular/core';

import {
  ITreeOptions,
  TreeComponent,
  TreeModel,
  TreeNode,
  TREE_ACTIONS
} from 'angular-tree-component';
import * as _ from 'lodash';

import { TableComponent } from '../../../shared/datatable/table/table.component';
import { Icons } from '../../../shared/enum/icons.enum';
import { CdTableColumn } from '../../../shared/models/cd-table-column';
import { BooleanTextPipe } from '../../../shared/pipes/boolean-text.pipe';
import { IscsiBackstorePipe } from '../../../shared/pipes/iscsi-backstore.pipe';

@Component({
  selector: 'cd-iscsi-target-details',
  templateUrl: './iscsi-target-details.component.html',
  styleUrls: ['./iscsi-target-details.component.scss']
})
export class IscsiTargetDetailsComponent implements OnChanges, OnInit {
  @Input()
  selection: any;
  @Input()
  settings: any;
  @Input()
  cephIscsiConfigVersion: number;

  @ViewChild('highlightTpl', { static: true })
  highlightTpl: TemplateRef<any>;

  private detailTable: TableComponent;
  @ViewChild('detailTable')
  set content(content: TableComponent) {
    this.detailTable = content;
    if (content) {
      content.updateColumns();
    }
  }

  @ViewChild('tree') tree: TreeComponent;

  icons = Icons;
  columns: CdTableColumn[];
  data: any;
  metadata: any = {};
  selectedItem: any;
  title: string;

  nodes: any[] = [];
  treeOptions: ITreeOptions = {
    useVirtualScroll: true,
    actionMapping: {
      mouse: {
        click: this.onNodeSelected.bind(this)
      }
    }
  };

  constructor(
    private iscsiBackstorePipe: IscsiBackstorePipe,
    private booleanTextPipe: BooleanTextPipe
  ) {}

  ngOnInit() {
    this.columns = [
      {
        prop: 'displayName',
        name: $localize`Name`,
        flexGrow: 1,
        cellTemplate: this.highlightTpl
      },
      {
        prop: 'current',
        name: $localize`Current`,
        flexGrow: 1,
        cellTemplate: this.highlightTpl
      },
      {
        prop: 'default',
        name: $localize`Default`,
        flexGrow: 1,
        cellTemplate: this.highlightTpl
      }
    ];
  }

  ngOnChanges() {
    if (this.selection) {
      this.selectedItem = this.selection;
      this.generateTree();
    }

    this.data = undefined;
  }

  private generateTree() {
    const target_meta = _.cloneDeep(this.selectedItem.target_controls);
    // Target level authentication was introduced in ceph-iscsi config v11
    if (this.cephIscsiConfigVersion > 10) {
      _.extend(target_meta, _.cloneDeep(this.selectedItem.auth));
    }
    this.metadata = { root: target_meta };
    const cssClasses = {
      target: {
        expanded: _.join(
          this.selectedItem.cdExecuting
            ? [Icons.large, Icons.spinner, Icons.spin]
            : [Icons.large, Icons.bullseye],
          ' '
        )
      },
      initiators: {
        expanded: _.join([Icons.large, Icons.user], ' '),
        leaf: _.join([Icons.user], ' ')
      },
      groups: {
        expanded: _.join([Icons.large, Icons.users], ' '),
        leaf: _.join([Icons.users], ' ')
      },
      disks: {
        expanded: _.join([Icons.large, Icons.disk], ' '),
        leaf: _.join([Icons.disk], ' ')
      },
      portals: {
        expanded: _.join([Icons.large, Icons.server], ' '),
        leaf: _.join([Icons.server], ' ')
      }
    };

    const disks: any[] = [];
    _.forEach(this.selectedItem.disks, (disk) => {
      const cdId = 'disk_' + disk.pool + '_' + disk.image;
      this.metadata[cdId] = {
        controls: disk.controls,
        backstore: disk.backstore
      };
      ['wwn', 'lun'].forEach((k) => {
        if (k in disk) {
          this.metadata[cdId][k] = disk[k];
        }
      });
      disks.push({
        name: `${disk.pool}/${disk.image}`,
        cdId: cdId,
        cdIcon: cssClasses.disks.leaf
      });
    });

    const portals: any[] = [];
    _.forEach(this.selectedItem.portals, (portal) => {
      portals.push({
        name: `${portal.host}:${portal.ip}`,
        cdIcon: cssClasses.portals.leaf
      });
    });

    const clients: any[] = [];
    _.forEach(this.selectedItem.clients, (client) => {
      const client_metadata = _.cloneDeep(client.auth);
      if (client.info) {
        _.extend(client_metadata, client.info);
        delete client_metadata['state'];
        _.forEach(Object.keys(client.info.state), (state) => {
          client_metadata[state.toLowerCase()] = client.info.state[state];
        });
      }
      this.metadata['client_' + client.client_iqn] = client_metadata;

      const luns: any[] = [];
      client.luns.forEach((lun: Record<string, any>) => {
        luns.push({
          name: `${lun.pool}/${lun.image}`,
          cdId: 'disk_' + lun.pool + '_' + lun.image,
          cdIcon: cssClasses.disks.leaf
        });
      });

      let status = '';
      if (client.info) {
        status = Object.keys(client.info.state).includes('LOGGED_IN') ? 'logged_in' : 'logged_out';
      }
      clients.push({
        name: client.client_iqn,
        status: status,
        cdId: 'client_' + client.client_iqn,
        children: luns,
        cdIcon: cssClasses.initiators.leaf
      });
    });

    const groups: any[] = [];
    _.forEach(this.selectedItem.groups, (group) => {
      const luns: any[] = [];
      group.disks.forEach((disk: Record<string, any>) => {
        luns.push({
          name: `${disk.pool}/${disk.image}`,
          cdId: 'disk_' + disk.pool + '_' + disk.image,
          cdIcon: cssClasses.disks.leaf
        });
      });

      const initiators: any[] = [];
      group.members.forEach((member: string) => {
        initiators.push({
          name: member,
          cdId: 'client_' + member
        });
      });

      groups.push({
        name: group.group_id,
        cdIcon: cssClasses.groups.leaf,
        children: [
          {
            name: 'Disks',
            children: luns,
            cdIcon: cssClasses.disks.expanded
          },
          {
            name: 'Initiators',
            children: initiators,
            cdIcon: cssClasses.initiators.expanded
          }
        ]
      });
    });

    this.nodes = [
      {
        name: this.selectedItem.target_iqn,
        cdId: 'root',
        isExpanded: true,
        cdIcon: cssClasses.target.expanded,
        children: [
          {
            name: 'Disks',
            isExpanded: true,
            children: disks,
            cdIcon: cssClasses.disks.expanded
          },
          {
            name: 'Portals',
            isExpanded: true,
            children: portals,
            cdIcon: cssClasses.portals.expanded
          },
          {
            name: 'Initiators',
            isExpanded: true,
            children: clients,
            cdIcon: cssClasses.initiators.expanded
          },
          {
            name: 'Groups',
            isExpanded: true,
            children: groups,
            cdIcon: cssClasses.groups.expanded
          }
        ]
      }
    ];
  }

  private format(value: any) {
    if (typeof value === 'boolean') {
      return this.booleanTextPipe.transform(value);
    }
    return value;
  }

  onNodeSelected(tree: TreeModel, node: TreeNode) {
    TREE_ACTIONS.ACTIVATE(tree, node, true);
    if (node.data.cdId) {
      this.title = node.data.name;
      const tempData = this.metadata[node.data.cdId] || {};

      if (node.data.cdId === 'root') {
        this.detailTable?.toggleColumn({ prop: 'default', isHidden: true });
        this.data = _.map(this.settings.target_default_controls, (value, key) => {
          value = this.format(value);
          return {
            displayName: key,
            default: value,
            current: !_.isUndefined(tempData[key]) ? this.format(tempData[key]) : value
          };
        });
        // Target level authentication was introduced in ceph-iscsi config v11
        if (this.cephIscsiConfigVersion > 10) {
          ['user', 'password', 'mutual_user', 'mutual_password'].forEach((key) => {
            this.data.push({
              displayName: key,
              default: null,
              current: tempData[key]
            });
          });
        }
      } else if (node.data.cdId.toString().startsWith('disk_')) {
        this.detailTable?.toggleColumn({ prop: 'default', isHidden: true });
        this.data = _.map(this.settings.disk_default_controls[tempData.backstore], (value, key) => {
          value = this.format(value);
          return {
            displayName: key,
            default: value,
            current: !_.isUndefined(tempData.controls[key])
              ? this.format(tempData.controls[key])
              : value
          };
        });
        this.data.push({
          displayName: 'backstore',
          default: this.iscsiBackstorePipe.transform(this.settings.default_backstore),
          current: this.iscsiBackstorePipe.transform(tempData.backstore)
        });
        ['wwn', 'lun'].forEach((k) => {
          if (k in tempData) {
            this.data.push({
              displayName: k,
              default: undefined,
              current: tempData[k]
            });
          }
        });
      } else {
        this.detailTable?.toggleColumn({ prop: 'default', isHidden: false });
        this.data = _.map(tempData, (value, key) => {
          return {
            displayName: key,
            default: undefined,
            current: this.format(value)
          };
        });
      }
    } else {
      this.data = undefined;
    }

    this.detailTable?.updateColumns();
  }

  onUpdateData() {
    this.tree.treeModel.expandAll();
  }
}

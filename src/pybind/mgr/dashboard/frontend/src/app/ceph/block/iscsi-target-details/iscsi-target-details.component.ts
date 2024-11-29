import { Component, Input, OnChanges, OnInit, TemplateRef, ViewChild } from '@angular/core';

import { Node } from 'carbon-components-angular/treeview/tree-node.types';
import _ from 'lodash';

import { TableComponent } from '~/app/shared/datatable/table/table.component';
import { Icons } from '~/app/shared/enum/icons.enum';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { BooleanTextPipe } from '~/app/shared/pipes/boolean-text.pipe';
import { IscsiBackstorePipe } from '~/app/shared/pipes/iscsi-backstore.pipe';
import { TreeViewService } from '~/app/shared/services/tree-view.service';

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

  @ViewChild('treeNodeTemplate', { static: true }) labelTpl: TemplateRef<any>;

  icons = Icons;
  columns: CdTableColumn[];
  data: any;
  metadata: any = {};
  selectedItem: any;
  title: string;

  nodes: Node[] = [];

  constructor(
    private iscsiBackstorePipe: IscsiBackstorePipe,
    private booleanTextPipe: BooleanTextPipe,
    public treeViewService: TreeViewService
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
      const id = 'disk_' + disk.pool + '_' + disk.image;
      this.metadata[id] = {
        controls: disk.controls,
        backstore: disk.backstore
      };
      ['wwn', 'lun'].forEach((k) => {
        if (k in disk) {
          this.metadata[id][k] = disk[k];
        }
      });
      disks.push({
        id: id,
        name: `${disk.pool}/${disk.image}`,
        label: `${disk.pool}/${disk.image}`,
        value: { cdIcon: cssClasses.disks.leaf }
      });
    });

    const portals: Node[] = [];
    _.forEach(this.selectedItem.portals, (portal) => {
      portals.push({
        label: this.labelTpl,
        labelContext: {
          name: `${portal.host}:${portal.ip}`,
          cdIcon: cssClasses.portals.leaf
        },
        value: {
          name: `${portal.host}:${portal.ip}`,
          cdIcon: cssClasses.portals.leaf
        }
      });
    });

    const clients: Node[] = [];
    _.forEach(this.selectedItem.clients, (client: Node) => {
      const client_metadata = _.cloneDeep(client.auth);
      if (client.info) {
        _.extend(client_metadata, client.info);
        delete client_metadata['state'];
        _.forEach(Object.keys(client.info.state), (state) => {
          client_metadata[state.toLowerCase()] = client.info.state[state];
        });
      }
      this.metadata['client_' + client.client_iqn] = client_metadata;

      const luns: Node[] = [];
      client.luns.forEach((lun: Node) => {
        luns.push({
          label: this.labelTpl,
          labelContext: {
            name: `${lun.pool}/${lun.image}`,
            cdIcon: cssClasses.disks.leaf
          },
          value: {
            name: `${lun.pool}/${lun.image}`,
            cdIcon: cssClasses.disks.leaf
          },
          id: 'disk_' + lun.pool + '_' + lun.image
        });
      });

      let status = '';
      if (client.info) {
        status = Object.keys(client.info.state).includes('LOGGED_IN') ? 'logged_in' : 'logged_out';
      }
      clients.push({
        label: this.labelTpl,
        labelContext: {
          name: client.client_iqn,
          status: status,
          cdIcon: cssClasses.initiators.leaf
        },
        value: {
          name: client.client_iqn,
          status: status,
          cdIcon: cssClasses.initiators.leaf
        },
        id: 'client_' + client.client_iqn,
        children: luns
      });
    });

    const groups: Node[] = [];
    _.forEach(this.selectedItem.groups, (group: Node) => {
      const luns: Node[] = [];
      group.disks.forEach((disk: Node) => {
        luns.push({
          label: this.labelTpl,
          labelContext: {
            name: `${disk.pool}/${disk.image}`,
            cdIcon: cssClasses.disks.leaf
          },
          value: {
            name: `${disk.pool}/${disk.image}`,
            cdIcon: cssClasses.disks.leaf
          },
          id: 'disk_' + disk.pool + '_' + disk.image
        });
      });

      const initiators: Node[] = [];
      group.members.forEach((member: string) => {
        initiators.push({
          label: this.labelTpl,
          labelContext: { name: member },
          value: { name: member },
          id: 'client_' + member
        });
      });

      groups.push({
        label: this.labelTpl,
        labelContext: { name: group.group_id, cdIcon: cssClasses.groups.leaf },
        value: { name: group.group_id, cdIcon: cssClasses.groups.leaf },
        children: [
          {
            label: this.labelTpl,
            labelContext: { name: 'Disks', cdIcon: cssClasses.disks.expanded },
            value: { name: 'Disks', cdIcon: cssClasses.disks.expanded },
            children: luns
          },
          {
            label: this.labelTpl,
            labelContext: { name: 'Initiators', cdIcon: cssClasses.initiators.expanded },
            value: { name: 'Initiators', cdIcon: cssClasses.initiators.expanded },
            children: initiators
          }
        ]
      });
    });

    this.nodes = [
      {
        id: 'root',
        label: this.labelTpl,
        labelContext: {
          name: this.selectedItem.target_iqn,
          cdIcon: cssClasses.target.expanded
        },
        value: {
          name: this.selectedItem.target_iqn,
          cdIcon: cssClasses.target.expanded
        },
        expanded: true,
        children: [
          {
            label: this.labelTpl,
            labelContext: { name: 'Disks', cdIcon: cssClasses.disks.expanded },
            value: { name: 'Disks', cdIcon: cssClasses.disks.expanded },
            expanded: true,
            children: disks
          },
          {
            label: this.labelTpl,
            labelContext: { name: 'Portals', cdIcon: cssClasses.portals.expanded },
            value: { name: 'Portals', cdIcon: cssClasses.portals.expanded },
            expanded: true,
            children: portals
          },
          {
            label: this.labelTpl,
            labelContext: { name: 'Initiators', cdIcon: cssClasses.initiators.expanded },
            value: { name: 'Initiators', cdIcon: cssClasses.initiators.expanded },
            expanded: true,
            children: clients
          },
          {
            label: this.labelTpl,
            labelContext: { name: 'Groups', cdIcon: cssClasses.groups.expanded },
            value: { name: 'Groups', cdIcon: cssClasses.groups.expanded },
            expanded: true,
            children: groups
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

  onNodeSelected(node: Node) {
    if (node.id) {
      this.title = node?.value?.name;
      const tempData = this.metadata[node.id] || {};

      if (node.id === 'root') {
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
      } else if (node.id.toString().startsWith('disk_')) {
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
}

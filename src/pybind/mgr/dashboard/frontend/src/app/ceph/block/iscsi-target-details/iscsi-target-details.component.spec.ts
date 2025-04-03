import { ComponentFixture, TestBed } from '@angular/core/testing';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';

import { Node } from 'carbon-components-angular/treeview/tree-node.types';
import { TreeviewModule } from 'carbon-components-angular';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { IscsiTargetDetailsComponent } from './iscsi-target-details.component';

describe('IscsiTargetDetailsComponent', () => {
  let component: IscsiTargetDetailsComponent;
  let fixture: ComponentFixture<IscsiTargetDetailsComponent>;
  let tree: Node[] = [];

  configureTestBed({
    declarations: [IscsiTargetDetailsComponent],
    imports: [BrowserAnimationsModule, TreeviewModule, SharedModule]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(IscsiTargetDetailsComponent);
    component = fixture.componentInstance;

    component.settings = {
      config: { minimum_gateways: 2 },
      disk_default_controls: {
        'backstore:1': {
          hw_max_sectors: 1024,
          max_data_area_mb: 8
        },
        'backstore:2': {
          hw_max_sectors: 1024,
          max_data_area_mb: 8
        }
      },
      target_default_controls: {
        cmdsn_depth: 128,
        dataout_timeout: 20
      },
      backstores: ['backstore:1', 'backstore:2'],
      default_backstore: 'backstore:1'
    };
    component.selection = undefined;
    component.selection = {
      target_iqn: 'iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw',
      portals: [{ host: 'node1', ip: '192.168.100.201' }],
      disks: [
        {
          pool: 'rbd',
          image: 'disk_1',
          backstore: 'backstore:1',
          controls: { hw_max_sectors: 1 }
        }
      ],
      clients: [
        {
          client_iqn: 'iqn.1994-05.com.redhat:rh7-client',
          luns: [{ pool: 'rbd', image: 'disk_1' }],
          auth: {
            user: 'myiscsiusername'
          },
          info: {
            alias: 'myhost',
            ip_address: ['192.168.200.1'],
            state: { LOGGED_IN: ['node1'] }
          }
        }
      ],
      groups: [],
      target_controls: { dataout_timeout: 2 }
    };
    tree = [
      {
        label: component.labelTpl,
        labelContext: {
          cdIcon: 'fa fa-lg fa fa-bullseye',
          name: 'iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw'
        },
        value: {
          cdIcon: 'fa fa-lg fa fa-bullseye',
          name: 'iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw'
        },
        children: [
          {
            children: [
              {
                id: 'disk_rbd_disk_1',
                label: 'rbd/disk_1',
                name: 'rbd/disk_1',
                value: { cdIcon: 'fa fa-hdd-o' }
              }
            ],
            expanded: true,
            label: component.labelTpl,
            labelContext: { cdIcon: 'fa fa-lg fa fa-hdd-o', name: 'Disks' },
            value: { cdIcon: 'fa fa-lg fa fa-hdd-o', name: 'Disks' }
          },
          {
            children: [
              {
                label: 'node1:192.168.100.201',
                value: {
                  cdIcon: 'fa fa-server',
                  name: 'node1:192.168.100.201'
                }
              }
            ],
            expanded: true,
            label: component.labelTpl,
            labelContext: { cdIcon: 'fa fa-lg fa fa-server', name: 'Portals' },
            value: { cdIcon: 'fa fa-lg fa fa-server', name: 'Portals' }
          },
          {
            children: [
              {
                id: 'client_iqn.1994-05.com.redhat:rh7-client',
                label: component.labelTpl,
                labelContext: {
                  cdIcon: 'fa fa-user',
                  name: 'iqn.1994-05.com.redhat:rh7-client',
                  status: 'logged_in'
                },
                value: {
                  cdIcon: 'fa fa-user',
                  name: 'iqn.1994-05.com.redhat:rh7-client',
                  status: 'logged_in'
                },
                children: [
                  {
                    id: 'disk_rbd_disk_1',
                    label: component.labelTpl,
                    labelContext: {
                      cdIcon: 'fa fa-hdd-o',
                      name: 'rbd/disk_1'
                    },
                    value: {
                      cdIcon: 'fa fa-hdd-o',
                      name: 'rbd/disk_1'
                    }
                  }
                ]
              }
            ],
            expanded: true,
            label: component.labelTpl,
            labelContext: { cdIcon: 'fa fa-lg fa fa-user', name: 'Initiators' },
            value: { cdIcon: 'fa fa-lg fa fa-user', name: 'Initiators' }
          },
          {
            children: [],
            expanded: true,
            label: component.labelTpl,
            labelContext: { cdIcon: 'fa fa-lg fa fa-users', name: 'Groups' },
            value: { cdIcon: 'fa fa-lg fa fa-users', name: 'Groups' }
          }
        ],
        expanded: true,
        id: 'root'
      }
    ];
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should empty data and generateTree when ngOnChanges is called', () => {
    const tempData = [{ current: 'baz', default: 'bar', displayName: 'foo' }];
    component.data = tempData;
    fixture.detectChanges();

    expect(component.data).toEqual(tempData);
    expect(component.metadata).toEqual({});
    expect(component.nodes).toEqual([]);

    component.ngOnChanges();

    expect(component.data).toBeUndefined();
    expect(component.metadata).toEqual({
      'client_iqn.1994-05.com.redhat:rh7-client': {
        user: 'myiscsiusername',
        alias: 'myhost',
        ip_address: ['192.168.200.1'],
        logged_in: ['node1']
      },
      disk_rbd_disk_1: { backstore: 'backstore:1', controls: { hw_max_sectors: 1 } },
      root: { dataout_timeout: 2 }
    });
    expect(component.nodes[0].label).toEqual(component.labelTpl);
    expect(component.nodes[0].labelContext).toEqual({
      cdIcon: 'fa fa-lg fa fa-bullseye',
      name: 'iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw'
    });
    expect(component.nodes).toHaveLength(1);
    expect(component.nodes[0].children).toHaveLength(4);
    // Commenting out the assertion below due to error:
    // "TypeError: 'caller', 'callee', and 'arguments' properties may not be accessed on strict mode functions or the arguments objects for calls to them"
    // Apparently an error that (hopefully) has been fixed in later version of Angular
    //
    // expect(component.nodes).toEqual(tree);
  });

  describe('should update data when onNodeSelected is called', () => {
    beforeEach(() => {
      component.nodes = tree;
      component.ngOnChanges();
      fixture.detectChanges();
    });

    it('with target selected', () => {
      const node = component.treeViewService.findNode('root', component.nodes);
      component.onNodeSelected(node);
      expect(component.data).toEqual([
        { current: 128, default: 128, displayName: 'cmdsn_depth' },
        { current: 2, default: 20, displayName: 'dataout_timeout' }
      ]);
    });

    it('with disk selected', () => {
      const node = component.treeViewService.findNode('disk_rbd_disk_1', component.nodes);
      component.onNodeSelected(node);
      expect(component.data).toEqual([
        { current: 1, default: 1024, displayName: 'hw_max_sectors' },
        { current: 8, default: 8, displayName: 'max_data_area_mb' },
        { current: 'backstore:1', default: 'backstore:1', displayName: 'backstore' }
      ]);
    });

    it('with initiator selected', () => {
      const node = component.treeViewService.findNode(
        'client_iqn.1994-05.com.redhat:rh7-client',
        component.nodes
      );
      component.onNodeSelected(node);
      expect(component.data).toEqual([
        { current: 'myiscsiusername', default: undefined, displayName: 'user' },
        { current: 'myhost', default: undefined, displayName: 'alias' },
        { current: ['192.168.200.1'], default: undefined, displayName: 'ip_address' },
        { current: ['node1'], default: undefined, displayName: 'logged_in' }
      ]);
    });

    it('with any other selected', () => {
      const node = component.treeViewService.findNode('Disks', component.nodes, 'value.name');
      component.onNodeSelected(node);
      expect(component.data).toBeUndefined();
    });
  });
});

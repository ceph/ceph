import { ComponentFixture, TestBed } from '@angular/core/testing';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';

import { TreeModel, TreeModule } from 'angular-tree-component';
import * as _ from 'lodash';

import { configureTestBed } from '../../../../testing/unit-test-helper';
import { SharedModule } from '../../../shared/shared.module';
import { IscsiTargetDetailsComponent } from './iscsi-target-details.component';

describe('IscsiTargetDetailsComponent', () => {
  let component: IscsiTargetDetailsComponent;
  let fixture: ComponentFixture<IscsiTargetDetailsComponent>;

  configureTestBed({
    declarations: [IscsiTargetDetailsComponent],
    imports: [BrowserAnimationsModule, TreeModule.forRoot(), SharedModule]
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
    expect(component.nodes).toEqual([
      {
        cdIcon: 'fa fa-lg fa fa-bullseye',
        cdId: 'root',
        children: [
          {
            cdIcon: 'fa fa-lg fa fa-hdd-o',
            children: [
              {
                cdIcon: 'fa fa-hdd-o',
                cdId: 'disk_rbd_disk_1',
                name: 'rbd/disk_1'
              }
            ],
            isExpanded: true,
            name: 'Disks'
          },
          {
            cdIcon: 'fa fa-lg fa fa-server',
            children: [
              {
                cdIcon: 'fa fa-server',
                name: 'node1:192.168.100.201'
              }
            ],
            isExpanded: true,
            name: 'Portals'
          },
          {
            cdIcon: 'fa fa-lg fa fa-user',
            children: [
              {
                cdIcon: 'fa fa-user',
                cdId: 'client_iqn.1994-05.com.redhat:rh7-client',
                children: [
                  {
                    cdIcon: 'fa fa-hdd-o',
                    cdId: 'disk_rbd_disk_1',
                    name: 'rbd/disk_1'
                  }
                ],
                name: 'iqn.1994-05.com.redhat:rh7-client',
                status: 'logged_in'
              }
            ],
            isExpanded: true,
            name: 'Initiators'
          },
          {
            cdIcon: 'fa fa-lg fa fa-users',
            children: [],
            isExpanded: true,
            name: 'Groups'
          }
        ],
        isExpanded: true,
        name: 'iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw'
      }
    ]);
  });

  describe('should update data when onNodeSelected is called', () => {
    let tree: TreeModel;

    beforeEach(() => {
      component.ngOnChanges();
      tree = component.tree.treeModel;
      fixture.detectChanges();
    });

    it('with target selected', () => {
      const node = tree.getNodeBy({ data: { cdId: 'root' } });
      component.onNodeSelected(tree, node);
      expect(component.data).toEqual([
        { current: 128, default: 128, displayName: 'cmdsn_depth' },
        { current: 2, default: 20, displayName: 'dataout_timeout' }
      ]);
    });

    it('with disk selected', () => {
      const node = tree.getNodeBy({ data: { cdId: 'disk_rbd_disk_1' } });
      component.onNodeSelected(tree, node);
      expect(component.data).toEqual([
        { current: 1, default: 1024, displayName: 'hw_max_sectors' },
        { current: 8, default: 8, displayName: 'max_data_area_mb' },
        { current: 'backstore:1', default: 'backstore:1', displayName: 'backstore' }
      ]);
    });

    it('with initiator selected', () => {
      const node = tree.getNodeBy({ data: { cdId: 'client_iqn.1994-05.com.redhat:rh7-client' } });
      component.onNodeSelected(tree, node);
      expect(component.data).toEqual([
        { current: 'myiscsiusername', default: undefined, displayName: 'user' },
        { current: 'myhost', default: undefined, displayName: 'alias' },
        { current: ['192.168.200.1'], default: undefined, displayName: 'ip_address' },
        { current: ['node1'], default: undefined, displayName: 'logged_in' }
      ]);
    });

    it('with any other selected', () => {
      const node = tree.getNodeBy({ data: { name: 'Disks' } });
      component.onNodeSelected(tree, node);
      expect(component.data).toBeUndefined();
    });
  });
});

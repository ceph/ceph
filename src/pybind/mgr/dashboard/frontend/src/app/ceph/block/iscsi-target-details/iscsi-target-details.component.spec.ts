import { ComponentFixture, TestBed } from '@angular/core/testing';

import { NodeEvent, Tree, TreeModule } from 'ng2-tree';

import { configureTestBed, i18nProviders } from '../../../../testing/unit-test-helper';
import { CdTableSelection } from '../../../shared/models/cd-table-selection';
import { SharedModule } from '../../../shared/shared.module';
import { IscsiTargetDetailsComponent } from './iscsi-target-details.component';

import * as _ from 'lodash';
import { Icons } from '../../../shared/enum/icons.enum';

describe('IscsiTargetDetailsComponent', () => {
  let component: IscsiTargetDetailsComponent;
  let fixture: ComponentFixture<IscsiTargetDetailsComponent>;

  configureTestBed({
    declarations: [IscsiTargetDetailsComponent],
    imports: [TreeModule, SharedModule],
    providers: [i18nProviders]
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
    component.selection = new CdTableSelection();
    component.selection.selected = [
      {
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
    expect(component.tree).toEqual(undefined);

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
    expect(component.tree).toEqual({
      children: [
        {
          children: [{ id: 'disk_rbd_disk_1', value: 'rbd/disk_1' }],
          settings: {
            cssClasses: {
              expanded: _.join([Icons.large, Icons.disk], ' '),
              leaf: _.join([Icons.disk], ' ')
            },
            selectionAllowed: false
          },
          value: 'Disks'
        },
        {
          children: [{ value: 'node1:192.168.100.201' }],
          settings: {
            cssClasses: {
              expanded: _.join([Icons.large, Icons.server], ' '),
              leaf: _.join([Icons.large, Icons.server], ' ')
            },
            selectionAllowed: false
          },
          value: 'Portals'
        },
        {
          children: [
            {
              children: [
                {
                  id: 'disk_rbd_disk_1',
                  settings: {
                    cssClasses: {
                      expanded: _.join([Icons.large, Icons.disk], ' '),
                      leaf: _.join([Icons.disk], ' ')
                    }
                  },
                  value: 'rbd/disk_1'
                }
              ],
              id: 'client_iqn.1994-05.com.redhat:rh7-client',
              status: 'logged_in',
              value: 'iqn.1994-05.com.redhat:rh7-client'
            }
          ],
          settings: {
            cssClasses: {
              expanded: _.join([Icons.large, Icons.user], ' '),
              leaf: _.join([Icons.user], ' ')
            },
            selectionAllowed: false
          },
          value: 'Initiators'
        },
        {
          children: [],
          settings: {
            cssClasses: {
              expanded: _.join([Icons.large, Icons.user], ' '),
              leaf: _.join([Icons.user], ' ')
            },
            selectionAllowed: false
          },
          value: 'Groups'
        }
      ],
      id: 'root',
      settings: {
        cssClasses: { expanded: _.join([Icons.large, Icons.bullseye], ' ') },
        static: true
      },
      value: 'iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw'
    });
  });

  describe('should update data when onNodeSelected is called', () => {
    beforeEach(() => {
      component.ngOnChanges();
    });

    it('with target selected', () => {
      const tree = new Tree(component.tree);
      const node = new NodeEvent(tree);
      component.onNodeSelected(node);
      expect(component.data).toEqual([
        { current: 128, default: 128, displayName: 'cmdsn_depth' },
        { current: 2, default: 20, displayName: 'dataout_timeout' }
      ]);
    });

    it('with disk selected', () => {
      const tree = new Tree(component.tree.children[0].children[0]);
      const node = new NodeEvent(tree);
      component.onNodeSelected(node);
      expect(component.data).toEqual([
        { current: 1, default: 1024, displayName: 'hw_max_sectors' },
        { current: 8, default: 8, displayName: 'max_data_area_mb' },
        { current: 'backstore:1', default: 'backstore:1', displayName: 'backstore' }
      ]);
    });

    it('with initiator selected', () => {
      const tree = new Tree(component.tree.children[2].children[0]);
      const node = new NodeEvent(tree);
      component.onNodeSelected(node);
      expect(component.data).toEqual([
        { current: 'myiscsiusername', default: undefined, displayName: 'user' },
        { current: 'myhost', default: undefined, displayName: 'alias' },
        { current: ['192.168.200.1'], default: undefined, displayName: 'ip_address' },
        { current: ['node1'], default: undefined, displayName: 'logged_in' }
      ]);
    });

    it('with any other selected', () => {
      const tree = new Tree(component.tree.children[1].children[0]);
      const node = new NodeEvent(tree);
      component.onNodeSelected(node);
      expect(component.data).toBeUndefined();
    });
  });
});

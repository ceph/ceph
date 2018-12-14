import { ComponentFixture, TestBed } from '@angular/core/testing';

import { NodeEvent, Tree, TreeModule } from 'ng2-tree';

import { configureTestBed, i18nProviders } from '../../../../testing/unit-test-helper';
import { CdTableSelection } from '../../../shared/models/cd-table-selection';
import { SharedModule } from '../../../shared/shared.module';
import { IscsiTargetDetailsComponent } from './iscsi-target-details.component';

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
        hw_max_sectors: 1024,
        max_data_area_mb: 8
      },
      target_default_controls: {
        cmdsn_depth: 128,
        dataout_timeout: 20
      }
    };
    component.selection = new CdTableSelection();
    component.selection.selected = [
      {
        target_iqn: 'iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw',
        portals: [{ host: 'node1', ip: '192.168.100.201' }],
        disks: [{ pool: 'rbd', image: 'disk_1', controls: { hw_max_sectors: 1 } }],
        clients: [
          {
            client_iqn: 'iqn.1994-05.com.redhat:rh7-client',
            luns: [{ pool: 'rbd', image: 'disk_1' }],
            auth: {
              user: 'myiscsiusername'
            }
          }
        ],
        groups: [],
        target_controls: { dataout_timeout: 2 }
      }
    ];
    component.selection.update();

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
      'client_iqn.1994-05.com.redhat:rh7-client': { user: 'myiscsiusername' },
      disk_rbd_disk_1: { hw_max_sectors: 1 },
      root: { dataout_timeout: 2 }
    });
    expect(component.tree).toEqual({
      children: [
        {
          children: [{ id: 'disk_rbd_disk_1', value: 'rbd/disk_1' }],
          settings: {
            cssClasses: { expanded: 'fa fa-fw fa-hdd-o fa-lg', leaf: 'fa fa-fw fa-hdd-o' },
            selectionAllowed: false
          },
          value: 'Disks'
        },
        {
          children: [{ value: 'node1:192.168.100.201' }],
          settings: {
            cssClasses: { expanded: 'fa fa-fw fa-server fa-lg', leaf: 'fa fa-fw fa-server fa-lg' },
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
                    cssClasses: { expanded: 'fa fa-fw fa-hdd-o fa-lg', leaf: 'fa fa-fw fa-hdd-o' }
                  },
                  value: 'rbd/disk_1'
                }
              ],
              id: 'client_iqn.1994-05.com.redhat:rh7-client',
              value: 'iqn.1994-05.com.redhat:rh7-client'
            }
          ],
          settings: {
            cssClasses: { expanded: 'fa fa-fw fa-user fa-lg', leaf: 'fa fa-fw fa-user' },
            selectionAllowed: false
          },
          value: 'Initiators'
        },
        {
          children: [],
          settings: {
            cssClasses: { expanded: 'fa fa-fw fa-users fa-lg', leaf: 'fa fa-fw fa-users' },
            selectionAllowed: false
          },
          value: 'Groups'
        }
      ],
      id: 'root',
      settings: { cssClasses: { expanded: 'fa fa-fw fa-bullseye fa-lg' }, static: true },
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
        { current: 8, default: 8, displayName: 'max_data_area_mb' }
      ]);
    });

    it('with initiator selected', () => {
      const tree = new Tree(component.tree.children[2].children[0]);
      const node = new NodeEvent(tree);
      component.onNodeSelected(node);
      expect(component.data).toEqual([
        { current: 'myiscsiusername', default: undefined, displayName: 'user' }
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

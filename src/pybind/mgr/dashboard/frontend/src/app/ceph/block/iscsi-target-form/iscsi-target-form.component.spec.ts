import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { ActivatedRoute } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';

import { ToastModule } from 'ng2-toastr';

import { ActivatedRouteStub } from '../../../../testing/activated-route-stub';
import { configureTestBed, i18nProviders } from '../../../../testing/unit-test-helper';
import { SharedModule } from '../../../shared/shared.module';
import { IscsiTargetFormComponent } from './iscsi-target-form.component';

describe('IscsiTargetFormComponent', () => {
  let component: IscsiTargetFormComponent;
  let fixture: ComponentFixture<IscsiTargetFormComponent>;
  let httpTesting: HttpTestingController;
  let activatedRoute: ActivatedRouteStub;

  const SETTINGS = {
    config: { minimum_gateways: 2 },
    disk_default_controls: {
      'backstore:1': {
        hw_max_sectors: 1024,
        osd_op_timeout: 30
      },
      'backstore:2': {
        qfull_timeout: 5
      }
    },
    target_default_controls: {
      cmdsn_depth: 128,
      dataout_timeout: 20,
      immediate_data: 'Yes'
    },
    required_rbd_features: {
      'backstore:1': 0,
      'backstore:2': 0
    },
    supported_rbd_features: {
      'backstore:1': 61,
      'backstore:2': 61
    },
    backstores: ['backstore:1', 'backstore:2'],
    default_backstore: 'backstore:1'
  };

  const LIST_TARGET = [
    {
      target_iqn: 'iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw',
      portals: [{ host: 'node1', ip: '192.168.100.201' }],
      disks: [{ pool: 'rbd', image: 'disk_1', controls: {}, backstore: 'backstore:1' }],
      clients: [
        {
          client_iqn: 'iqn.1994-05.com.redhat:rh7-client',
          luns: [{ pool: 'rbd', image: 'disk_1' }],
          auth: {
            user: 'myiscsiusername',
            password: 'myiscsipassword',
            mutual_user: null,
            mutual_password: null
          }
        }
      ],
      groups: [],
      target_controls: {}
    }
  ];

  const PORTALS = [
    { name: 'node1', ip_addresses: ['192.168.100.201', '10.0.2.15'] },
    { name: 'node2', ip_addresses: ['192.168.100.202'] }
  ];

  const RBD_LIST = [
    { status: 0, value: [], pool_name: 'ganesha' },
    {
      status: 0,
      value: [
        {
          size: 96636764160,
          obj_size: 4194304,
          num_objs: 23040,
          order: 22,
          block_name_prefix: 'rbd_data.148162fb31a8',
          name: 'disk_1',
          id: '148162fb31a8',
          pool_name: 'rbd',
          features: 61,
          features_name: ['deep-flatten', 'exclusive-lock', 'fast-diff', 'layering', 'object-map'],
          timestamp: '2019-01-18T10:44:26Z',
          stripe_count: 1,
          stripe_unit: 4194304,
          data_pool: null,
          parent: null,
          snapshots: [],
          total_disk_usage: 0,
          disk_usage: 0
        },
        {
          size: 119185342464,
          obj_size: 4194304,
          num_objs: 28416,
          order: 22,
          block_name_prefix: 'rbd_data.14b292cee6cb',
          name: 'disk_2',
          id: '14b292cee6cb',
          pool_name: 'rbd',
          features: 61,
          features_name: ['deep-flatten', 'exclusive-lock', 'fast-diff', 'layering', 'object-map'],
          timestamp: '2019-01-18T10:45:56Z',
          stripe_count: 1,
          stripe_unit: 4194304,
          data_pool: null,
          parent: null,
          snapshots: [],
          total_disk_usage: 0,
          disk_usage: 0
        }
      ],
      pool_name: 'rbd'
    }
  ];

  configureTestBed(
    {
      declarations: [IscsiTargetFormComponent],
      imports: [
        SharedModule,
        ReactiveFormsModule,
        HttpClientTestingModule,
        RouterTestingModule,
        ToastModule.forRoot()
      ],
      providers: [
        i18nProviders,
        {
          provide: ActivatedRoute,
          useValue: new ActivatedRouteStub({ target_iqn: undefined })
        }
      ]
    },
    true
  );

  beforeEach(() => {
    fixture = TestBed.createComponent(IscsiTargetFormComponent);
    component = fixture.componentInstance;
    httpTesting = TestBed.get(HttpTestingController);
    activatedRoute = TestBed.get(ActivatedRoute);
    fixture.detectChanges();

    httpTesting.expectOne('ui-api/iscsi/settings').flush(SETTINGS);
    httpTesting.expectOne('ui-api/iscsi/portals').flush(PORTALS);
    httpTesting.expectOne('api/summary').flush({});
    httpTesting.expectOne('api/block/image').flush(RBD_LIST);
    httpTesting.expectOne('api/iscsi/target').flush(LIST_TARGET);
    httpTesting.verify();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should only show images not used in other targets', () => {
    expect(component.imagesAll).toEqual([RBD_LIST[1]['value'][1]]);
    expect(component.imagesSelections).toEqual([
      { description: '', name: 'rbd/disk_2', selected: false }
    ]);
  });

  it('should generate portals selectOptions', () => {
    expect(component.portalsSelections).toEqual([
      { description: '', name: 'node1:192.168.100.201', selected: false },
      { description: '', name: 'node1:10.0.2.15', selected: false },
      { description: '', name: 'node2:192.168.100.202', selected: false }
    ]);
  });

  it('should create the form', () => {
    expect(component.targetForm.value).toEqual({
      disks: [],
      groups: [],
      initiators: [],
      acl_enabled: false,
      portals: [],
      target_controls: {},
      target_iqn: component.targetForm.value.target_iqn
    });
  });

  it('should prepare data when selecting an image', () => {
    expect(component.imagesSettings).toEqual({});
    component.onImageSelection({ option: { name: 'rbd/disk_2', selected: true } });
    expect(component.imagesSettings).toEqual({
      'rbd/disk_2': {
        backstore: 'backstore:1',
        'backstore:1': {}
      }
    });
  });

  it('should clean data when removing an image', () => {
    component.onImageSelection({ option: { name: 'rbd/disk_2', selected: true } });
    component.addGroup();
    component.groups.controls[0].patchValue({
      group_id: 'foo',
      disks: ['rbd/disk_2']
    });

    expect(component.groups.controls[0].value).toEqual({
      disks: ['rbd/disk_2'],
      group_id: 'foo',
      members: []
    });

    component.onImageSelection({ option: { name: 'rbd/disk_2', selected: false } });

    expect(component.groups.controls[0].value).toEqual({ disks: [], group_id: 'foo', members: [] });
    expect(component.imagesSettings).toEqual({
      'rbd/disk_2': {
        backstore: 'backstore:1',
        'backstore:1': {}
      }
    });
  });

  describe('should test initiators', () => {
    beforeEach(() => {
      component.targetForm.patchValue({ disks: ['rbd/disk_2'], acl_enabled: true });
      component.addGroup().patchValue({ name: 'group_1' });
      component.onImageSelection({ option: { name: 'rbd/disk_2', selected: true } });

      component.addInitiator();
      component.initiators.controls[0].patchValue({
        client_iqn: 'iqn.initiator'
      });
      component.updatedInitiatorSelector();
    });

    it('should prepare data when creating an initiator', () => {
      expect(component.initiators.controls.length).toBe(1);
      expect(component.initiators.controls[0].value).toEqual({
        auth: { mutual_password: '', mutual_user: '', password: '', user: '' },
        cdIsInGroup: false,
        client_iqn: 'iqn.initiator',
        luns: []
      });
      expect(component.imagesInitiatorSelections).toEqual([
        [{ description: '', name: 'rbd/disk_2', selected: false }]
      ]);
      expect(component.groupMembersSelections).toEqual([
        [{ description: '', name: 'iqn.initiator', selected: false }]
      ]);
    });

    it('should update data when changing an initiator name', () => {
      expect(component.groupMembersSelections).toEqual([
        [{ description: '', name: 'iqn.initiator', selected: false }]
      ]);

      component.initiators.controls[0].patchValue({
        client_iqn: 'iqn.initiator_new'
      });
      component.updatedInitiatorSelector();

      expect(component.groupMembersSelections).toEqual([
        [{ description: '', name: 'iqn.initiator_new', selected: false }]
      ]);
    });

    it('should clean data when removing an initiator', () => {
      component.groups.controls[0].patchValue({
        group_id: 'foo',
        members: ['iqn.initiator']
      });

      expect(component.groups.controls[0].value).toEqual({
        disks: [],
        group_id: 'foo',
        members: ['iqn.initiator']
      });

      component.removeInitiator(0);

      expect(component.groups.controls[0].value).toEqual({
        disks: [],
        group_id: 'foo',
        members: []
      });
      expect(component.groupMembersSelections).toEqual([[]]);
      expect(component.imagesInitiatorSelections).toEqual([]);
    });

    it('should remove images in the initiator when added in a group', () => {
      component.initiators.controls[0].patchValue({
        luns: ['rbd/disk_2']
      });
      expect(component.initiators.controls[0].value).toEqual({
        auth: { mutual_password: '', mutual_user: '', password: '', user: '' },
        cdIsInGroup: false,
        client_iqn: 'iqn.initiator',
        luns: ['rbd/disk_2']
      });

      component.addGroup();
      component.groups.controls[0].patchValue({
        group_id: 'foo',
        members: ['iqn.initiator']
      });
      component.onGroupMemberSelection({
        option: {
          name: 'iqn.initiator',
          selected: true
        }
      });

      expect(component.initiators.controls[0].value).toEqual({
        auth: { mutual_password: '', mutual_user: '', password: '', user: '' },
        cdIsInGroup: true,
        client_iqn: 'iqn.initiator',
        luns: []
      });
    });
  });

  describe('should submit request', () => {
    beforeEach(() => {
      component.targetForm.patchValue({ disks: ['rbd/disk_2'], acl_enabled: true });
      component.onImageSelection({ option: { name: 'rbd/disk_2', selected: true } });
      component.portals.setValue(['node1:192.168.100.201', 'node2:192.168.100.202']);
      component.addInitiator().patchValue({
        client_iqn: 'iqn.initiator'
      });
      component.addGroup().patchValue({
        group_id: 'foo',
        members: ['iqn.initiator'],
        disks: ['rbd/disk_2']
      });
    });

    it('should call update', () => {
      activatedRoute.setParams({ target_iqn: 'iqn.iscsi' });
      component.isEdit = true;
      component.target_iqn = 'iqn.iscsi';

      component.submit();

      const req = httpTesting.expectOne('api/iscsi/target/iqn.iscsi');
      expect(req.request.method).toBe('PUT');
      expect(req.request.body).toEqual({
        clients: [
          {
            auth: { mutual_password: null, mutual_user: null, password: null, user: null },
            cdIsInGroup: false,
            client_iqn: 'iqn.initiator',
            luns: []
          }
        ],
        disks: [{ backstore: 'backstore:1', controls: {}, image: 'disk_2', pool: 'rbd' }],
        groups: [
          { disks: [{ image: 'disk_2', pool: 'rbd' }], group_id: 'foo', members: ['iqn.initiator'] }
        ],
        new_target_iqn: component.targetForm.value.target_iqn,
        portals: [
          { host: 'node1', ip: '192.168.100.201' },
          { host: 'node2', ip: '192.168.100.202' }
        ],
        target_controls: {},
        target_iqn: component.target_iqn,
        acl_enabled: true
      });
    });

    it('should call create', () => {
      component.submit();

      const req = httpTesting.expectOne('api/iscsi/target');
      expect(req.request.method).toBe('POST');
      expect(req.request.body).toEqual({
        clients: [
          {
            auth: { mutual_password: null, mutual_user: null, password: null, user: null },
            cdIsInGroup: false,
            client_iqn: 'iqn.initiator',
            luns: []
          }
        ],
        disks: [{ backstore: 'backstore:1', controls: {}, image: 'disk_2', pool: 'rbd' }],
        groups: [
          {
            disks: [{ image: 'disk_2', pool: 'rbd' }],
            group_id: 'foo',
            members: ['iqn.initiator']
          }
        ],
        portals: [
          { host: 'node1', ip: '192.168.100.201' },
          { host: 'node2', ip: '192.168.100.202' }
        ],
        target_controls: {},
        target_iqn: component.targetForm.value.target_iqn,
        acl_enabled: true
      });
    });

    it('should call create with acl_enabled disabled', () => {
      component.targetForm.patchValue({ acl_enabled: false });
      component.submit();

      const req = httpTesting.expectOne('api/iscsi/target');
      expect(req.request.method).toBe('POST');
      expect(req.request.body).toEqual({
        clients: [],
        disks: [{ backstore: 'backstore:1', controls: {}, image: 'disk_2', pool: 'rbd' }],
        groups: [],
        acl_enabled: false,
        portals: [
          { host: 'node1', ip: '192.168.100.201' },
          { host: 'node2', ip: '192.168.100.202' }
        ],
        target_controls: {},
        target_iqn: component.targetForm.value.target_iqn
      });
    });
  });
});

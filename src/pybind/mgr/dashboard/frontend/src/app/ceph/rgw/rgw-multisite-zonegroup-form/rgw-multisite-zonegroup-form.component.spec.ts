import { HttpClientTestingModule } from '@angular/common/http/testing';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';
import { of as observableOf } from 'rxjs';

import { RgwZonegroupService } from '~/app/shared/api/rgw-zonegroup.service';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { NotificationService } from '~/app/shared/services/notification.service';

import { RgwMultisiteZonegroupFormComponent } from './rgw-multisite-zonegroup-form.component';

const defaultMultisiteInfo = [{ realms: [] }, { zonegroups: [] }, { zones: [] }];
const defaultDefaultsInfo = {
  defaultRealmName: null,
  defaultZonegroupName: null,
  defaultZoneName: null
};

const editInfo = {
  data: {
    name: 'zg-1',
    zones: [{ id: 'z1', name: 'zone-1' }],
    master_zone: 'z1',
    endpoints: [],
    placement_targets: [],
    is_default: false,
    is_master: false,
    parent: 'realm1'
  }
};

describe('RgwMultisiteZonegroupFormComponent', () => {
  let component: RgwMultisiteZonegroupFormComponent;
  let fixture: ComponentFixture<RgwMultisiteZonegroupFormComponent>;
  let notificationService: NotificationService;
  let rgwZonegroupService: RgwZonegroupService;

  const createComponent = async (
    action = 'Create',
    info: any = null,
    multisiteInfo = defaultMultisiteInfo,
    defaultsInfo: any = defaultDefaultsInfo
  ) => {
    await TestBed.configureTestingModule({
      imports: [ReactiveFormsModule, RouterTestingModule, HttpClientTestingModule],
      declarations: [RgwMultisiteZonegroupFormComponent],
      providers: [
        ActionLabelsI18n,
        CdFormBuilder,
        { provide: 'action', useValue: action },
        { provide: 'resource', useValue: 'zonegroup' },
        { provide: 'info', useValue: info },
        { provide: 'multisiteInfo', useValue: multisiteInfo },
        { provide: 'defaultsInfo', useValue: defaultsInfo },
        { provide: NotificationService, useValue: { show: jest.fn() } },
        { provide: RgwZonegroupService, useValue: { create: jest.fn(), update: jest.fn() } }
      ],
      schemas: [NO_ERRORS_SCHEMA]
    }).compileComponents();

    notificationService = TestBed.inject(NotificationService);
    rgwZonegroupService = TestBed.inject(RgwZonegroupService);

    fixture = TestBed.createComponent(RgwMultisiteZonegroupFormComponent);
    component = fixture.componentInstance;
    jest.spyOn(component, 'closeModal').mockImplementation(() => {});
    component.ngOnInit();
  };

  const setFormForSubmit = (endpoints: string) => {
    component.multisiteZonegroupForm.get('zonegroupName').setValue('zg-1');
    component.multisiteZonegroupForm.get('zonegroup_endpoints').setValue(endpoints);
    component.multisiteZonegroupForm.markAsDirty();
  };

  beforeEach(() => TestBed.resetTestingModule());

  afterEach(() => jest.clearAllMocks());

  it('should create', async () => {
    await createComponent();
    expect(component).toBeTruthy();
  });

  describe('form validation', () => {
    it('should have a sync validator on zonegroupName', async () => {
      await createComponent();
      expect(typeof component.multisiteZonegroupForm.get('zonegroupName').validator).toBe(
        'function'
      );
    });

    it('should have no async validator on zonegroupName in edit mode', async () => {
      await createComponent('Edit', editInfo);
      expect(component.multisiteZonegroupForm.get('zonegroupName').asyncValidator).toBeNull();
    });
  });

  describe('submit', () => {
    it('should show success notification on create', async () => {
      await createComponent();
      (rgwZonegroupService.create as jest.Mock).mockReturnValue(observableOf({}));
      setFormForSubmit('http://192.1.1.1:8004');

      component.submit();

      expect(notificationService.show).toHaveBeenCalledWith(
        NotificationType.success,
        expect.stringContaining('zg-1')
      );
    });

    it('should show success notification on update', async () => {
      await createComponent('Edit', editInfo);
      (rgwZonegroupService.update as jest.Mock).mockReturnValue(observableOf({}));
      component.zgZoneNames = ['zone-1'];
      component.zonegroupZoneNames = ['zone-1'];
      setFormForSubmit('http://192.1.1.1:8004,http://192.12.12.12:8004');

      component.submit();

      expect(notificationService.show).toHaveBeenCalledWith(
        NotificationType.success,
        expect.stringContaining('zg-1')
      );
    });
  });
});

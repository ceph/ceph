import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, fakeAsync, TestBed, tick } from '@angular/core/testing';
import { FormControl, ReactiveFormsModule } from '@angular/forms';
import { Router } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbTooltipModule } from '@ng-bootstrap/ng-bootstrap';
import { ToastrModule } from 'ngx-toastr';
import { of as observableOf } from 'rxjs';

import { configureTestBed, FormHelper } from '../../../../testing/unit-test-helper';
import { RgwUserService } from '../../../shared/api/rgw-user.service';
import { NotificationType } from '../../../shared/enum/notification-type.enum';
import { NotificationService } from '../../../shared/services/notification.service';
import { SharedModule } from '../../../shared/shared.module';
import { RgwUserCapabilities } from '../models/rgw-user-capabilities';
import { RgwUserCapability } from '../models/rgw-user-capability';
import { RgwUserS3Key } from '../models/rgw-user-s3-key';
import { RgwUserFormComponent } from './rgw-user-form.component';

describe('RgwUserFormComponent', () => {
  let component: RgwUserFormComponent;
  let fixture: ComponentFixture<RgwUserFormComponent>;
  let rgwUserService: RgwUserService;
  let formHelper: FormHelper;

  configureTestBed({
    declarations: [RgwUserFormComponent],
    imports: [
      HttpClientTestingModule,
      ReactiveFormsModule,
      RouterTestingModule,
      SharedModule,
      ToastrModule.forRoot(),
      NgbTooltipModule
    ]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RgwUserFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
    rgwUserService = TestBed.inject(RgwUserService);
    formHelper = new FormHelper(component.userForm);
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('s3 key management', () => {
    beforeEach(() => {
      spyOn(rgwUserService, 'addS3Key').and.stub();
    });

    it('should not update key', () => {
      component.setS3Key(new RgwUserS3Key(), 3);
      expect(component.s3Keys.length).toBe(0);
      expect(rgwUserService.addS3Key).not.toHaveBeenCalled();
    });

    it('should set user defined key', () => {
      const key = new RgwUserS3Key();
      key.user = 'test1:subuser2';
      key.access_key = 'my-access-key';
      key.secret_key = 'my-secret-key';
      component.setS3Key(key);
      expect(component.s3Keys.length).toBe(1);
      expect(component.s3Keys[0].user).toBe('test1:subuser2');
      expect(rgwUserService.addS3Key).toHaveBeenCalledWith('test1', {
        subuser: 'subuser2',
        generate_key: 'false',
        access_key: 'my-access-key',
        secret_key: 'my-secret-key'
      });
    });

    it('should set params for auto-generating key', () => {
      const key = new RgwUserS3Key();
      key.user = 'test1:subuser2';
      key.generate_key = true;
      key.access_key = 'my-access-key';
      key.secret_key = 'my-secret-key';
      component.setS3Key(key);
      expect(component.s3Keys.length).toBe(1);
      expect(component.s3Keys[0].user).toBe('test1:subuser2');
      expect(rgwUserService.addS3Key).toHaveBeenCalledWith('test1', {
        subuser: 'subuser2',
        generate_key: 'true'
      });
    });

    it('should set key w/o subuser', () => {
      const key = new RgwUserS3Key();
      key.user = 'test1';
      component.setS3Key(key);
      expect(component.s3Keys.length).toBe(1);
      expect(component.s3Keys[0].user).toBe('test1');
      expect(rgwUserService.addS3Key).toHaveBeenCalledWith('test1', {
        subuser: '',
        generate_key: 'false',
        access_key: undefined,
        secret_key: undefined
      });
    });
  });

  describe('quotaMaxSizeValidator', () => {
    it('should validate max size (1)', () => {
      const resp = component.quotaMaxSizeValidator(new FormControl(''));
      expect(resp).toBe(null);
    });

    it('should validate max size (2)', () => {
      const resp = component.quotaMaxSizeValidator(new FormControl('xxxx'));
      expect(resp.quotaMaxSize).toBeTruthy();
    });

    it('should validate max size (3)', () => {
      const resp = component.quotaMaxSizeValidator(new FormControl('1023'));
      expect(resp.quotaMaxSize).toBeTruthy();
    });

    it('should validate max size (4)', () => {
      const resp = component.quotaMaxSizeValidator(new FormControl('1024'));
      expect(resp).toBe(null);
    });

    it('should validate max size (5)', () => {
      const resp = component.quotaMaxSizeValidator(new FormControl('1M'));
      expect(resp).toBe(null);
    });

    it('should validate max size (6)', () => {
      const resp = component.quotaMaxSizeValidator(new FormControl('1024 gib'));
      expect(resp).toBe(null);
    });

    it('should validate max size (7)', () => {
      const resp = component.quotaMaxSizeValidator(new FormControl('10 X'));
      expect(resp.quotaMaxSize).toBeTruthy();
    });

    it('should validate max size (8)', () => {
      const resp = component.quotaMaxSizeValidator(new FormControl('1.085 GiB'));
      expect(resp).toBe(null);
    });

    it('should validate max size (9)', () => {
      const resp = component.quotaMaxSizeValidator(new FormControl('1,085 GiB'));
      expect(resp.quotaMaxSize).toBeTruthy();
    });
  });

  describe('username validation', () => {
    beforeEach(() => {
      spyOn(rgwUserService, 'enumerate').and.returnValue(observableOf(['abc', 'xyz']));
    });

    it('should validate that username is required', () => {
      formHelper.expectErrorChange('uid', '', 'required', true);
    });

    it('should validate that username is valid', fakeAsync(() => {
      formHelper.setValue('uid', 'ab', true);
      tick(500);
      formHelper.expectValid('uid');
    }));

    it('should validate that username is invalid', fakeAsync(() => {
      formHelper.setValue('uid', 'abc', true);
      tick(500);
      formHelper.expectError('uid', 'notUnique');
    }));
  });

  describe('max buckets', () => {
    it('disable creation (create)', () => {
      spyOn(rgwUserService, 'create');
      formHelper.setValue('max_buckets_mode', -1, true);
      component.onSubmit();
      expect(rgwUserService.create).toHaveBeenCalledWith({
        access_key: '',
        display_name: null,
        email: '',
        generate_key: true,
        max_buckets: -1,
        secret_key: '',
        suspended: false,
        uid: null
      });
    });

    it('disable creation (edit)', () => {
      spyOn(rgwUserService, 'update');
      component.editing = true;
      formHelper.setValue('max_buckets_mode', -1, true);
      component.onSubmit();
      expect(rgwUserService.update).toHaveBeenCalledWith(null, {
        display_name: null,
        email: null,
        max_buckets: -1,
        suspended: false
      });
    });

    it('unlimited buckets (create)', () => {
      spyOn(rgwUserService, 'create');
      formHelper.setValue('max_buckets_mode', 0, true);
      component.onSubmit();
      expect(rgwUserService.create).toHaveBeenCalledWith({
        access_key: '',
        display_name: null,
        email: '',
        generate_key: true,
        max_buckets: 0,
        secret_key: '',
        suspended: false,
        uid: null
      });
    });

    it('unlimited buckets (edit)', () => {
      spyOn(rgwUserService, 'update');
      component.editing = true;
      formHelper.setValue('max_buckets_mode', 0, true);
      component.onSubmit();
      expect(rgwUserService.update).toHaveBeenCalledWith(null, {
        display_name: null,
        email: null,
        max_buckets: 0,
        suspended: false
      });
    });

    it('custom (create)', () => {
      spyOn(rgwUserService, 'create');
      formHelper.setValue('max_buckets_mode', 1, true);
      formHelper.setValue('max_buckets', 100, true);
      component.onSubmit();
      expect(rgwUserService.create).toHaveBeenCalledWith({
        access_key: '',
        display_name: null,
        email: '',
        generate_key: true,
        max_buckets: 100,
        secret_key: '',
        suspended: false,
        uid: null
      });
    });

    it('custom (edit)', () => {
      spyOn(rgwUserService, 'update');
      component.editing = true;
      formHelper.setValue('max_buckets_mode', 1, true);
      formHelper.setValue('max_buckets', 100, true);
      component.onSubmit();
      expect(rgwUserService.update).toHaveBeenCalledWith(null, {
        display_name: null,
        email: null,
        max_buckets: 100,
        suspended: false
      });
    });
  });

  describe('submit form', () => {
    let notificationService: NotificationService;

    beforeEach(() => {
      spyOn(TestBed.inject(Router), 'navigate').and.stub();
      notificationService = TestBed.inject(NotificationService);
      spyOn(notificationService, 'show');
    });

    it('should be able to clear the mail field on update', () => {
      spyOn(rgwUserService, 'update');
      component.editing = true;
      formHelper.setValue('email', '', true);
      component.onSubmit();
      expect(rgwUserService.update).toHaveBeenCalledWith(null, {
        display_name: null,
        email: '',
        max_buckets: 1000,
        suspended: false
      });
    });

    it('tests create success notification', () => {
      spyOn(rgwUserService, 'create').and.returnValue(observableOf([]));
      component.editing = false;
      formHelper.setValue('suspended', true, true);
      component.onSubmit();
      expect(notificationService.show).toHaveBeenCalledWith(
        NotificationType.success,
        `Created Object Gateway user 'null'`
      );
    });

    it('tests update success notification', () => {
      spyOn(rgwUserService, 'update').and.returnValue(observableOf([]));
      component.editing = true;
      formHelper.setValue('suspended', true, true);
      component.onSubmit();
      expect(notificationService.show).toHaveBeenCalledWith(
        NotificationType.success,
        `Updated Object Gateway user 'null'`
      );
    });
  });

  describe('RgwUserCapabilities', () => {
    it('capability button disabled when all capabilities are added', () => {
      component.editing = true;
      for (const capabilityType of RgwUserCapabilities.getAll()) {
        const capability = new RgwUserCapability();
        capability.type = capabilityType;
        capability.perm = 'read';
        component.setCapability(capability);
      }

      fixture.detectChanges();

      expect(component.hasAllCapabilities()).toBeTruthy();
      const capabilityButton = fixture.debugElement.nativeElement.querySelector('.tc_addCapButton');
      expect(capabilityButton.disabled).toBeTruthy();
    });

    it('capability button not disabled when not all capabilities are added', () => {
      component.editing = true;

      fixture.detectChanges();

      const capabilityButton = fixture.debugElement.nativeElement.querySelector('.tc_addCapButton');
      expect(capabilityButton.disabled).toBeFalsy();
    });
  });
});

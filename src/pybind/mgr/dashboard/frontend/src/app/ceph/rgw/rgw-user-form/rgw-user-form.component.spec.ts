import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, fakeAsync, TestBed, tick } from '@angular/core/testing';
import { FormControl, ReactiveFormsModule } from '@angular/forms';
import { Router } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbTooltipModule } from '@ng-bootstrap/ng-bootstrap';
import { PipesModule } from '~/app/shared/pipes/pipes.module';

import { defer, of as observableOf, throwError } from 'rxjs';

import { RgwUserService } from '~/app/shared/api/rgw-user.service';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { NotificationService } from '~/app/shared/services/notification.service';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed, FormHelper } from '~/testing/unit-test-helper';
import { RgwUserCapabilities } from '../models/rgw-user-capabilities';
import { RgwUserCapability } from '../models/rgw-user-capability';
import { RgwUserS3Key } from '../models/rgw-user-s3-key';
import { RgwUserFormComponent } from './rgw-user-form.component';
import { DUE_TIMER } from '~/app/shared/forms/cd-validators';
import { FormatterService } from '~/app/shared/services/formatter.service';
import { RgwRateLimitComponent } from '../rgw-rate-limit/rgw-rate-limit.component';
import { By } from '@angular/platform-browser';
import { CheckboxModule, NumberModule, SelectModule } from 'carbon-components-angular';
import { LoadingStatus } from '~/app/shared/forms/cd-form';
import { USER } from '~/app/shared/constants/app.constants';

describe('RgwUserFormComponent', () => {
  let component: RgwUserFormComponent;
  let fixture: ComponentFixture<RgwUserFormComponent>;
  let rgwUserService: RgwUserService;
  let formHelper: FormHelper;
  let modalRef: any;
  let childComponent: any;
  configureTestBed({
    declarations: [RgwUserFormComponent, RgwRateLimitComponent],
    imports: [
      HttpClientTestingModule,
      ReactiveFormsModule,
      RouterTestingModule,
      SharedModule,
      NgbTooltipModule,
      PipesModule,
      CheckboxModule,
      NumberModule,
      SelectModule
    ]
  });

  beforeEach(async () => {
    fixture = TestBed.createComponent(RgwUserFormComponent);
    component = fixture.componentInstance;
    rgwUserService = TestBed.inject(RgwUserService);
    formHelper = new FormHelper(component.userForm);
    await fixture.whenStable();
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
    it('should validate that username is required', () => {
      formHelper.expectErrorChange('user_id', '', 'required', true);
    });

    it('should validate that username is valid', fakeAsync(() => {
      spyOn(rgwUserService, 'get').and.returnValue(throwError('foo'));
      formHelper.setValue('user_id', 'ab', true);
      tick(DUE_TIMER);
      formHelper.expectValid('user_id');
    }));

    it('should validate that username can contain dot(.)', fakeAsync(() => {
      spyOn(rgwUserService, 'get').and.returnValue(throwError('foo'));
      formHelper.setValue('user_id', 'user.name', true);
      tick(DUE_TIMER);
      formHelper.expectValid('user_id');
    }));

    it('should validate that username is invalid', fakeAsync(() => {
      spyOn(rgwUserService, 'get').and.returnValue(observableOf({}));
      formHelper.setValue('user_id', 'abc', true);
      tick(DUE_TIMER);
      formHelper.expectError('user_id', 'notUnique');
    }));
  });

  describe('tenant validation', () => {
    it('should validate that tenant is valid', fakeAsync(() => {
      spyOn(rgwUserService, 'get').and.returnValue(throwError('foo'));
      formHelper.setValue('show_tenant', true, true);
      formHelper.setValue('tenant', 'new_tenant123', true);
      tick(DUE_TIMER);
      formHelper.expectValid('tenant');
    }));

    it('should validate that tenant is invalid', fakeAsync(() => {
      spyOn(rgwUserService, 'get').and.returnValue(observableOf({}));
      formHelper.setValue('show_tenant', true, true);
      formHelper.setValue('tenant', 'new-tenant.dummy', true);
      tick(DUE_TIMER);
      formHelper.expectError('tenant', 'pattern');
    }));
  });

  describe('max buckets', () => {
    beforeEach(() => {
      component.loading = LoadingStatus.Ready;
      fixture.detectChanges();
      childComponent = fixture.debugElement.query(
        By.directive(RgwRateLimitComponent)
      ).componentInstance;
    });
    it('disable creation (create)', () => {
      spyOn(rgwUserService, 'create');
      formHelper.setValue('max_buckets_mode', -1, true);
      let spyRateLimit = jest.spyOn(childComponent, 'getRateLimitFormValue');
      component.onSubmit();
      expect(rgwUserService.create).toHaveBeenCalledWith({
        access_key: '',
        display_name: null,
        email: '',
        generate_key: true,
        max_buckets: -1,
        secret_key: '',
        suspended: false,
        system: false,
        uid: null
      });
      expect(spyRateLimit).toHaveBeenCalled();
    });

    it('disable creation (edit)', () => {
      spyOn(rgwUserService, 'update');
      component.editing = true;
      formHelper.setValue('max_buckets_mode', -1, true);
      let spyRateLimit = jest.spyOn(childComponent, 'getRateLimitFormValue');
      component.onSubmit();
      expect(rgwUserService.update).toHaveBeenCalledWith(null, {
        display_name: null,
        email: null,
        max_buckets: -1,
        suspended: false,
        system: false
      });
      expect(spyRateLimit).toHaveBeenCalled();
    });

    it('unlimited buckets (create)', () => {
      spyOn(rgwUserService, 'create');
      formHelper.setValue('max_buckets_mode', 0, true);
      let spyRateLimit = jest.spyOn(childComponent, 'getRateLimitFormValue');
      component.onSubmit();
      expect(rgwUserService.create).toHaveBeenCalledWith({
        access_key: '',
        display_name: null,
        email: '',
        generate_key: true,
        max_buckets: 0,
        secret_key: '',
        suspended: false,
        system: false,
        uid: null
      });
      expect(spyRateLimit).toHaveBeenCalled();
    });

    it('unlimited buckets (edit)', () => {
      spyOn(rgwUserService, 'update');
      component.editing = true;
      formHelper.setValue('max_buckets_mode', 0, true);
      let spyRateLimit = jest.spyOn(childComponent, 'getRateLimitFormValue');
      component.onSubmit();
      expect(rgwUserService.update).toHaveBeenCalledWith(null, {
        display_name: null,
        email: null,
        max_buckets: 0,
        suspended: false,
        system: false
      });
      expect(spyRateLimit).toHaveBeenCalled();
    });

    it('custom (create)', () => {
      spyOn(rgwUserService, 'create');
      formHelper.setValue('max_buckets_mode', 1, true);
      formHelper.setValue('max_buckets', 100, true);
      let spyRateLimit = jest.spyOn(childComponent, 'getRateLimitFormValue');

      component.onSubmit();
      expect(rgwUserService.create).toHaveBeenCalledWith({
        access_key: '',
        display_name: null,
        email: '',
        generate_key: true,
        max_buckets: 100,
        secret_key: '',
        suspended: false,
        system: false,
        uid: null
      });
      expect(spyRateLimit).toHaveBeenCalled();
    });

    it('custom (edit)', () => {
      spyOn(rgwUserService, 'update');
      component.editing = true;
      formHelper.setValue('max_buckets_mode', 1, true);
      formHelper.setValue('max_buckets', 100, true);
      let spyRateLimit = jest.spyOn(childComponent, 'getRateLimitFormValue');
      component.onSubmit();
      expect(rgwUserService.update).toHaveBeenCalledWith(null, {
        display_name: null,
        email: null,
        max_buckets: 100,
        suspended: false,
        system: false
      });
      expect(spyRateLimit).toHaveBeenCalled();
    });
  });

  describe('submit form', () => {
    let notificationService: NotificationService;

    beforeEach(() => {
      component.loading = LoadingStatus.Ready;
      spyOn(TestBed.inject(Router), 'navigate').and.stub();
      notificationService = TestBed.inject(NotificationService);
      spyOn(notificationService, 'show');
      fixture.detectChanges();
      let childComponent = fixture.debugElement.query(
        By.directive(RgwRateLimitComponent)
      ).componentInstance;
      jest.spyOn(childComponent, 'getRateLimitFormValue');
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
        suspended: false,
        system: false
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

    it('should not replay stale requests when re-submitting after a failure', () => {
      const createSpy = spyOn(rgwUserService, 'create').and.returnValues(
        throwError('invalid tenant'),
        observableOf([])
      );
      component.editing = false;
      formHelper.setValue('user_id', 'userid', true);
      formHelper.setValue('tenant', 'tenant()', true);
      component.onSubmit();
      expect(notificationService.show).not.toHaveBeenCalled();

      formHelper.setValue('tenant', 'tenant1', true);
      component.onSubmit();

      expect(createSpy).toHaveBeenCalledTimes(2);
      expect(createSpy.calls.argsFor(0)[0]['uid']).toBe('tenant()$userid');
      expect(createSpy.calls.argsFor(1)[0]['uid']).toBe('tenant1$userid');
      expect(notificationService.show).toHaveBeenCalledWith(
        NotificationType.success,
        `Created Object Gateway user 'tenant1$userid'`
      );
    });

    it('should not re-run queued dialog requests that already succeeded', () => {
      const addCapSpy = spyOn(rgwUserService, 'addCapability').and.returnValue(observableOf([]));
      spyOn(rgwUserService, 'create').and.returnValues(
        throwError('invalid tenant'),
        observableOf([])
      );
      component.editing = false;
      formHelper.setValue('user_id', 'userid', true);
      const cap = new RgwUserCapability();
      cap.type = 'users';
      cap.perm = 'read';
      component.setCapability(cap);

      component.onSubmit();
      expect(component.submitObservables.length).toBe(0);
      component.onSubmit();

      expect(addCapSpy).toHaveBeenCalledTimes(1);
    });

    it('should use the latest form values for every field when re-submitting after a failure (create)', () => {
      const createSpy = spyOn(rgwUserService, 'create').and.returnValues(
        throwError('error'),
        throwError('error'),
        observableOf([])
      );
      component.editing = false;
      formHelper.setValue('user_id', 'userid', true);
      formHelper.setValue('display_name', 'first', true);
      formHelper.setValue('email', 'first@example.com', true);
      component.onSubmit();

      // Change a different field between failures.
      formHelper.setValue('display_name', 'second', true);
      component.onSubmit();

      // Change several fields, revert one back to its earlier value.
      formHelper.setValue('display_name', 'third', true);
      formHelper.setValue('email', '', true);
      component.onSubmit();

      expect(createSpy).toHaveBeenCalledTimes(3);
      expect(createSpy.calls.argsFor(0)[0]).toEqual(
        expect.objectContaining({ display_name: 'first', email: 'first@example.com' })
      );
      expect(createSpy.calls.argsFor(1)[0]).toEqual(
        expect.objectContaining({ display_name: 'second', email: 'first@example.com' })
      );
      expect(createSpy.calls.argsFor(2)[0]).toEqual(
        expect.objectContaining({ display_name: 'third', email: '' })
      );
    });

    it('should use the latest values and keep edits when re-submitting after a failure (update)', () => {
      const updateSpy = spyOn(rgwUserService, 'update').and.returnValues(
        throwError('error'),
        observableOf([])
      );
      component.editing = true;
      formHelper.setValue('display_name', 'first', true);
      component.onSubmit();

      // A previously untouched field is changed before retrying.
      formHelper.setValue('email', 'new@example.com', true);
      component.onSubmit();

      expect(updateSpy).toHaveBeenCalledTimes(2);
      expect(updateSpy.calls.argsFor(0)[1]).toEqual(
        expect.objectContaining({ display_name: 'first', email: null })
      );
      expect(updateSpy.calls.argsFor(1)[1]).toEqual(
        expect.objectContaining({ display_name: 'first', email: 'new@example.com' })
      );
    });

    it('should only run queued dialog requests that did not succeed, plus new ones added between failures', () => {
      // Count how often each request is actually subscribed (i.e. sent).
      const sent: Record<string, number> = { users: 0, buckets: 0, metadata: 0, create: 0 };
      let bucketsFails = true;
      spyOn(rgwUserService, 'addCapability').and.callFake((_uid: string, type: string) =>
        defer(() => {
          sent[type]++;
          if (type === 'buckets' && bucketsFails) {
            return throwError('error');
          }
          return observableOf([]);
        })
      );
      spyOn(rgwUserService, 'create').and.callFake(() =>
        defer(() => {
          sent['create']++;
          return observableOf([]);
        })
      );
      component.editing = false;
      formHelper.setValue('user_id', 'userid', true);
      const capA = new RgwUserCapability();
      capA.type = 'users';
      capA.perm = 'read';
      const capB = new RgwUserCapability();
      capB.type = 'buckets';
      capB.perm = 'read';
      component.setCapability(capA);
      component.setCapability(capB);

      // capA succeeds, capB fails -> create is never sent.
      component.onSubmit();
      expect(sent).toEqual({ users: 1, buckets: 1, metadata: 0, create: 0 });
      expect(component.submitObservables.length).toBe(1);

      // User adds another capability between failures, and the failure is fixed.
      const capC = new RgwUserCapability();
      capC.type = 'metadata';
      capC.perm = 'read';
      component.setCapability(capC);
      bucketsFails = false;
      component.onSubmit();

      // capA is not re-sent; capB is retried; capC and create are sent once.
      expect(sent).toEqual({ users: 1, buckets: 2, metadata: 1, create: 1 });
      expect(component.submitObservables.length).toBe(0);
    });
  });

  describe('RgwUserCapabilities', () => {
    beforeEach(() => {
      component.loading = LoadingStatus.Ready;
    });
    it('capability button disabled when all capabilities are added', () => {
      component.editing = true;
      for (const capabilityType of RgwUserCapabilities.getAll()) {
        const capability = new RgwUserCapability();
        capability.type = capabilityType;
        capability.perm = 'read';
        component.setCapability(capability);
      }

      fixture.detectChanges();

      expect(component.hasAllCapabilities(component.capabilities)).toBeTruthy();
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
  it('should not modify max_buckets if mode is not "1"', () => {
    formHelper.setValue('max_buckets', '', false);
    component.onMaxBucketsModeChange('2');
    const patchVal = spyOn(component.userForm, 'patchValue');
    expect(patchVal).not.toHaveBeenCalled();
  });

  describe('updateFieldsWhenTenanted()', () => {
    it('should reset the form when showTenant is falsy', () => {
      component.showTenant = false;

      component.previousTenant = 'true';

      component.userForm.get('tenant').setValue('test1');
      component.userForm.get('user_id').setValue('user-123');

      component.updateFieldsWhenTenanted();
      expect(component.userForm.get('user_id').untouched).toBeTruthy(); // user_id should be untouched
      expect(component.userForm.get('tenant').value).toBe('true'); // tenant should be reset to
    });
  });

  it('should call deletecapab', () => {
    component.capabilities = [{ type: 'users', perm: 'read' }];
    component.deleteCapability(0);
    expect(component.capabilities.length).toBe(0);
    expect(component.userForm.dirty).toBeTruthy();
  });
  it('should call deleteS3Key', () => {
    component.s3Keys = [
      { user: 'test5$test11', access_key: 'A009', secret_key: 'ABCKEY', generate_key: true }
    ];
    component.deleteS3Key(0);
    expect(component.userForm.dirty).toBeTruthy();
  });

  it('should call showCapabilityModal', () => {
    const modalShowSpy = spyOn(component['modalService'], 'show').and.callFake(() => {
      modalRef = {
        setEditing: jest.fn(),
        setValues: jest.fn(),
        setCapabilities: jest.fn(),
        submitAction: { subscribe: jest.fn() }
      };
      return modalRef;
    });
    component.capabilities = [{ type: 'users', perm: 'read' }];
    component.showCapabilityModal(0);
    expect(modalShowSpy).toHaveBeenCalled();
  });
  it('should call showSwiftKeyModal', () => {
    const modalShowSpy = spyOn(component['modalService'], 'show').and.callFake(() => {
      modalRef = {
        setValues: jest.fn()
      };
      return modalRef;
    });
    component.swiftKeys = [
      { user: 'user1', secret_key: 'secret1' },
      { user: 'user2', secret_key: 'secret2' }
    ];
    component.showSwiftKeyModal(0);
    expect(modalShowSpy).toHaveBeenCalled();
  });
  it('should call showS3KeyModal', () => {
    const modalShowSpy = spyOn(component['modalService'], 'show').and.callFake(() => {
      modalRef = {
        setValues: jest.fn(),
        setViewing: jest.fn(),
        setUserCandidates: jest.fn(),
        submitAction: { subscribe: jest.fn() }
      };
      return modalRef;
    });
    component.s3Keys = [
      { user: 'test5$test11', access_key: 'A009', secret_key: 'ABCKEY', generate_key: true }
    ];
    component.showS3KeyModal(0);
    expect(modalShowSpy).toHaveBeenCalled();
  });

  it('should call _getS3KeyUserCandidates', () => {
    spyOn(component, 'getUID').and.returnValue('mockUID');
    component.s3Keys = [
      { user: 'mockUID', access_key: 'test', secret_key: 'TestKey', generate_key: true }
    ];
    const keycandidates = component['_getS3KeyUserCandidates']();
    const result = ['mockUID'];
    expect(keycandidates).toEqual(result);
  });

  describe('test case for _getBucketQuotaArgs', () => {
    it('should return correct result when quota values are specified', () => {
      // Using patchValue to set form values
      component.userForm.patchValue({
        bucket_quota_enabled: true,
        bucket_quota_max_size: 2048, // 2MB
        bucket_quota_max_objects: 10000,
        bucket_quota_max_size_unlimited: false, // Not unlimited
        bucket_quota_max_objects_unlimited: false // Not unlimited
      });
      const formatterServiceSpy = jest.spyOn(FormatterService.prototype, 'toBytes');
      // Mock the toBytes function to return 2048 (bytes)
      formatterServiceSpy.mockReturnValue(2048); // Convert 2MB to bytes (2048 KB)

      const result = component['_getBucketQuotaArgs']();

      expect(result).toEqual({
        quota_type: 'bucket',
        enabled: true,
        max_size_kb: '2', // 2048 bytes = 2KB
        max_objects: 10000
      });
    });
    it('should return correct result when quota values are specified', () => {
      // Using patchValue to set form values
      component.userForm.patchValue({
        bucket_quota_enabled: true,
        bucket_quota_max_size: 2048, // 2MB
        bucket_quota_max_objects: 10000,
        bucket_quota_max_size_unlimited: false, // Not unlimited
        bucket_quota_max_objects_unlimited: false // Not unlimited
      });

      const formatterServiceSpy = jest.spyOn(FormatterService.prototype, 'toBytes');
      // Mock the toBytes function to return 2048 (bytes)
      formatterServiceSpy.mockReturnValue(2048); // Convert 2MB to bytes (2048 KB)

      const result = component['_getBucketQuotaArgs']();

      expect(result).toEqual({
        quota_type: 'bucket',
        enabled: true,
        max_size_kb: '2', // 2048 bytes = 2KB
        max_objects: 10000
      });
    });
    it('should return default values for unlimited size and objects', () => {
      component.userForm.patchValue({
        bucket_quota_enabled: true,
        bucket_quota_max_size: 2048, // 2MB
        bucket_quota_max_objects: 10000,
        bucket_quota_max_size_unlimited: true, // Unlimited
        bucket_quota_max_objects_unlimited: true // Unlimited
      });

      const formatterServiceSpy = jest.spyOn(FormatterService.prototype, 'toBytes');
      formatterServiceSpy.mockReturnValue(2048); // Convert 2MB to bytes (2048 KB)

      const result = component['_getBucketQuotaArgs']();

      expect(result).toEqual({
        quota_type: 'bucket',
        enabled: true,
        max_size_kb: -1, // Default value when unlimited
        max_objects: -1 // Default value when unlimited
      });
    });
  });
  describe('test case for _getUserQuotaArgs', () => {
    it('should return quota info with default values when no quota is set', () => {
      // Use patchValue to set form values
      component.userForm.patchValue({
        user_quota_enabled: true,
        user_quota_max_size_unlimited: true,
        user_quota_max_objects_unlimited: true,
        user_quota_max_size: null,
        user_quota_max_objects: null
      });

      // Call the method
      const result = component._getUserQuotaArgs();

      // Assertions
      expect(result).toEqual({
        quota_type: USER,
        enabled: true,
        max_size_kb: -1,
        max_objects: -1
      });
    });

    it('should calculate max_size_kb when quota size is specified and not unlimited', () => {
      // Use patchValue to set form values
      component.userForm.patchValue({
        user_quota_enabled: true,
        user_quota_max_size_unlimited: false,
        user_quota_max_objects_unlimited: true,
        user_quota_max_size: 2048, // Example quota size in KB
        user_quota_max_objects: null
      });

      const toBytesSpy = jest
        .spyOn(FormatterService.prototype, 'toBytes')
        .mockReturnValue(2048 * 1024);

      const result = component._getUserQuotaArgs();
      expect(toBytesSpy).toHaveBeenCalledWith(2048);
      expect(result).toEqual({
        quota_type: USER,
        enabled: true,
        max_size_kb: '2048', // Expect the converted KB value
        max_objects: -1
      });
    });

    it('should set max_objects when quota is specified and not unlimited', () => {
      // Use patchValue to set form values
      component.userForm.patchValue({
        user_quota_enabled: true,
        user_quota_max_size_unlimited: true,
        user_quota_max_objects_unlimited: false,
        user_quota_max_size: null,
        user_quota_max_objects: 1000 // Example quota size
      });

      const result = component._getUserQuotaArgs();

      expect(result).toEqual({
        quota_type: USER,
        enabled: true,
        max_size_kb: -1,
        max_objects: 1000
      });
    });
  });

  it('should call showSubuserModal', () => {
    const modalShowSpy = spyOn(component['modalService'], 'show').and.callFake(() => {
      modalRef = {
        setValues: jest.fn(),
        setViewing: jest.fn(),
        setEditing: jest.fn(),
        setUserCandidates: jest.fn(),
        submitAction: { subscribe: jest.fn() }
      };
      return modalRef;
    });
    component.subusers = [
      { id: 'test', permissions: 'true', generate_secret: true, secret_key: '' }
    ];
    //  component.s3Keys= [{user: 'test5$test11', access_key: 'A009', secret_key: 'ABCKEY', generate_key: true}];
    component.showSubuserModal(0);
    expect(modalShowSpy).toHaveBeenCalled();
  });
  describe('test case for showSubuserModal', () => {
    it('should handle "Edit" scenario when index is provided', () => {
      let index = 0;
      component.subusers = [
        { id: 'test', permissions: 'true', generate_secret: true, secret_key: '' }
      ];
      let spy = spyOn(component['modalService'], 'show').and.callFake(() => {
        return (modalRef = {
          setEditing: jest.fn(),
          setValues: jest.fn(),
          setCapabilities: jest.fn(),
          setSubusers: jest.fn(),
          setUserCandidates: jest.fn(),
          submitAction: { subscribe: jest.fn() }
        });
      });
      spyOn(component, 'getUID').and.returnValue('dashboard');
      component.showSubuserModal(index);
      expect(spy).toHaveBeenCalledTimes(1);
      expect(modalRef.setEditing).toHaveBeenCalledTimes(1);
      expect(modalRef.setValues).toHaveBeenCalledWith(
        'dashboard',
        component.subusers[index].id,
        component.subusers[index].permissions
      );
      expect(modalRef.submitAction.subscribe).toHaveBeenCalled();
    });

    it('should handle "Add" scenario when index is not provided', () => {
      let spy = spyOn(component['modalService'], 'show').and.callFake(() => {
        return (modalRef = {
          setEditing: jest.fn(),
          setValues: jest.fn(),
          setCapabilities: jest.fn(),
          setSubusers: jest.fn(),
          setUserCandidates: jest.fn(),
          submitAction: { subscribe: jest.fn() }
        });
      });
      component.subusers = [
        { id: 'test', permissions: 'true', generate_secret: true, secret_key: '' }
      ];
      spyOn(component, 'getUID').and.returnValue('dashboard');
      component.showSubuserModal();
      expect(spy).toHaveBeenCalledTimes(1);
      expect(modalRef.setEditing).toHaveBeenCalledWith(false);
      expect(modalRef.setValues).toHaveBeenCalledWith('dashboard');
      expect(modalRef.setSubusers).toHaveBeenCalledWith(component.subusers);
      expect(modalRef.submitAction.subscribe).toHaveBeenCalled();
    });
  });

  describe('RgwUserAccounts', () => {
    beforeEach(() => {
      component.loading = LoadingStatus.Ready;
      fixture.detectChanges();
      childComponent = fixture.debugElement.query(
        By.directive(RgwRateLimitComponent)
      ).componentInstance;
    });
    it('create with account id & account root user', () => {
      spyOn(rgwUserService, 'create');
      formHelper.setValue('account_id', 'RGW12312312312312312', true);
      formHelper.setValue('account_root_user', true, true);
      component.onSubmit();
      expect(rgwUserService.create).toHaveBeenCalledWith({
        access_key: '',
        display_name: null,
        email: '',
        generate_key: true,
        max_buckets: 1000,
        secret_key: '',
        suspended: false,
        system: false,
        uid: null,
        account_id: 'RGW12312312312312312',
        account_root_user: true
      });
    });

    it('edit to link account to existing user', () => {
      spyOn(rgwUserService, 'update');
      component.editing = true;
      formHelper.setValue('account_id', 'RGW12312312312312312', true);
      formHelper.setValue('account_root_user', true, true);
      component.onSubmit();
      expect(rgwUserService.update).toHaveBeenCalledWith(null, {
        display_name: null,
        email: null,
        max_buckets: 1000,
        suspended: false,
        system: false,
        account_id: 'RGW12312312312312312',
        account_root_user: true
      });
    });
  });
});

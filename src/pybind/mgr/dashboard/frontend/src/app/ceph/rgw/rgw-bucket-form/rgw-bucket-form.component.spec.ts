import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, fakeAsync, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { Router } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';

import _ from 'lodash';
import { ToastrModule } from 'ngx-toastr';
import { of as observableOf } from 'rxjs';

import { RgwBucketService } from '~/app/shared/api/rgw-bucket.service';
import { RgwSiteService } from '~/app/shared/api/rgw-site.service';
import { RgwUserService } from '~/app/shared/api/rgw-user.service';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { NotificationService } from '~/app/shared/services/notification.service';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed, FormHelper } from '~/testing/unit-test-helper';
import { RgwBucketMfaDelete } from '../models/rgw-bucket-mfa-delete';
import { RgwBucketVersioning } from '../models/rgw-bucket-versioning';
import { RgwBucketFormComponent } from './rgw-bucket-form.component';

describe('RgwBucketFormComponent', () => {
  let component: RgwBucketFormComponent;
  let fixture: ComponentFixture<RgwBucketFormComponent>;
  let rgwBucketService: RgwBucketService;
  let getPlacementTargetsSpy: jasmine.Spy;
  let rgwBucketServiceGetSpy: jasmine.Spy;
  let enumerateSpy: jasmine.Spy;
  let formHelper: FormHelper;

  configureTestBed({
    declarations: [RgwBucketFormComponent],
    imports: [
      HttpClientTestingModule,
      ReactiveFormsModule,
      RouterTestingModule,
      SharedModule,
      ToastrModule.forRoot()
    ]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RgwBucketFormComponent);
    component = fixture.componentInstance;
    rgwBucketService = TestBed.inject(RgwBucketService);
    rgwBucketServiceGetSpy = spyOn(rgwBucketService, 'get');
    getPlacementTargetsSpy = spyOn(TestBed.inject(RgwSiteService), 'get');
    enumerateSpy = spyOn(TestBed.inject(RgwUserService), 'enumerate');
    formHelper = new FormHelper(component.bucketForm);
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('bucketNameValidator', () => {
    it('should validate empty name', fakeAsync(() => {
      formHelper.expectErrorChange('bid', '', 'required', true);
    }));
  });

  describe('zonegroup and placement targets', () => {
    it('should get zonegroup and placement targets', () => {
      const payload: Record<string, any> = {
        zonegroup: 'default',
        placement_targets: [
          {
            name: 'default-placement',
            data_pool: 'default.rgw.buckets.data'
          },
          {
            name: 'placement-target2',
            data_pool: 'placement-target2.rgw.buckets.data'
          }
        ]
      };
      getPlacementTargetsSpy.and.returnValue(observableOf(payload));
      enumerateSpy.and.returnValue(observableOf([]));
      fixture.detectChanges();

      expect(component.zonegroup).toBe(payload.zonegroup);
      const placementTargets = [];
      for (const placementTarget of payload['placement_targets']) {
        placementTarget[
          'description'
        ] = `${placementTarget['name']} (pool: ${placementTarget['data_pool']})`;
        placementTargets.push(placementTarget);
      }
      expect(component.placementTargets).toEqual(placementTargets);
    });
  });

  describe('submit form', () => {
    let notificationService: NotificationService;

    beforeEach(() => {
      spyOn(TestBed.inject(Router), 'navigate').and.stub();
      notificationService = TestBed.inject(NotificationService);
      spyOn(notificationService, 'show');
    });

    it('should validate name', () => {
      component.editing = false;
      component.createForm();
      const control = component.bucketForm.get('bid');
      expect(_.isFunction(control.asyncValidator)).toBeTruthy();
    });

    it('should not validate name', () => {
      component.editing = true;
      component.createForm();
      const control = component.bucketForm.get('bid');
      expect(control.asyncValidator).toBeNull();
    });

    it('tests create success notification', () => {
      spyOn(rgwBucketService, 'create').and.returnValue(observableOf([]));
      component.editing = false;
      component.bucketForm.markAsDirty();
      component.submit();
      expect(notificationService.show).toHaveBeenCalledWith(
        NotificationType.success,
        `Created Object Gateway bucket 'null'`
      );
    });

    it('tests update success notification', () => {
      spyOn(rgwBucketService, 'update').and.returnValue(observableOf([]));
      component.editing = true;
      component.bucketForm.markAsDirty();
      component.submit();
      expect(notificationService.show).toHaveBeenCalledWith(
        NotificationType.success,
        `Updated Object Gateway bucket 'null'.`
      );
    });
  });

  describe('mfa credentials', () => {
    const checkMfaCredentialsVisibility = (
      fakeResponse: object,
      versioningChecked: boolean,
      mfaDeleteChecked: boolean,
      expectedVisibility: boolean
    ) => {
      component['route'].params = observableOf({ bid: 'bid' });
      component.editing = true;
      rgwBucketServiceGetSpy.and.returnValue(observableOf(fakeResponse));
      enumerateSpy.and.returnValue(observableOf([]));
      component.ngOnInit();
      component.bucketForm.patchValue({
        versioning: versioningChecked,
        'mfa-delete': mfaDeleteChecked
      });
      fixture.detectChanges();

      const mfaTokenSerial = fixture.debugElement.nativeElement.querySelector('#mfa-token-serial');
      const mfaTokenPin = fixture.debugElement.nativeElement.querySelector('#mfa-token-pin');
      if (expectedVisibility) {
        expect(mfaTokenSerial).toBeTruthy();
        expect(mfaTokenPin).toBeTruthy();
      } else {
        expect(mfaTokenSerial).toBeFalsy();
        expect(mfaTokenPin).toBeFalsy();
      }
    };

    it('inputs should be visible when required', () => {
      checkMfaCredentialsVisibility(
        {
          versioning: RgwBucketVersioning.SUSPENDED,
          mfa_delete: RgwBucketMfaDelete.DISABLED
        },
        false,
        false,
        false
      );
      checkMfaCredentialsVisibility(
        {
          versioning: RgwBucketVersioning.SUSPENDED,
          mfa_delete: RgwBucketMfaDelete.DISABLED
        },
        true,
        false,
        false
      );
      checkMfaCredentialsVisibility(
        {
          versioning: RgwBucketVersioning.ENABLED,
          mfa_delete: RgwBucketMfaDelete.DISABLED
        },
        false,
        false,
        false
      );
      checkMfaCredentialsVisibility(
        {
          versioning: RgwBucketVersioning.ENABLED,
          mfa_delete: RgwBucketMfaDelete.ENABLED
        },
        true,
        true,
        false
      );
      checkMfaCredentialsVisibility(
        {
          versioning: RgwBucketVersioning.SUSPENDED,
          mfa_delete: RgwBucketMfaDelete.DISABLED
        },
        false,
        true,
        true
      );
      checkMfaCredentialsVisibility(
        {
          versioning: RgwBucketVersioning.SUSPENDED,
          mfa_delete: RgwBucketMfaDelete.ENABLED
        },
        false,
        false,
        true
      );
      checkMfaCredentialsVisibility(
        {
          versioning: RgwBucketVersioning.SUSPENDED,
          mfa_delete: RgwBucketMfaDelete.ENABLED
        },
        true,
        true,
        true
      );
      checkMfaCredentialsVisibility(
        {
          versioning: RgwBucketVersioning.ENABLED,
          mfa_delete: RgwBucketMfaDelete.ENABLED
        },
        false,
        true,
        true
      );
    });
  });

  describe('object locking', () => {
    const expectPatternLockError = (value: string) => {
      formHelper.setValue('lock_enabled', true, true);
      formHelper.setValue('lock_retention_period_days', value);
      formHelper.expectError('lock_retention_period_days', 'pattern');
    };

    const expectValidLockInputs = (enabled: boolean, mode: string, days: string) => {
      formHelper.setValue('lock_enabled', enabled);
      formHelper.setValue('lock_mode', mode);
      formHelper.setValue('lock_retention_period_days', days);
      ['lock_enabled', 'lock_mode', 'lock_retention_period_days'].forEach((name) => {
        const control = component.bucketForm.get(name);
        expect(control.valid).toBeTruthy();
        expect(control.errors).toBeNull();
      });
    };

    it('should check lock enabled checkbox [mode=create]', () => {
      component.createForm();
      const control = component.bucketForm.get('lock_enabled');
      expect(control.disabled).toBeFalsy();
    });

    it('should check lock enabled checkbox [mode=edit]', () => {
      component.editing = true;
      component.createForm();
      const control = component.bucketForm.get('lock_enabled');
      expect(control.disabled).toBeTruthy();
    });

    it('should not have the "lockDays" error for 10 days', () => {
      formHelper.setValue('lock_enabled', true);
      const control = component.bucketForm.get('lock_retention_period_days');
      control.updateValueAndValidity();
      expect(control.value).toBe(10);
      expect(control.invalid).toBeFalsy();
      formHelper.expectValid(control);
    });

    it('should have the "lockDays" error for 0 days', () => {
      formHelper.setValue('lock_enabled', true);
      formHelper.setValue('lock_retention_period_days', 0);
      const control = component.bucketForm.get('lock_retention_period_days');
      control.updateValueAndValidity();
      expect(control.value).toBe(0);
      expect(control.invalid).toBeTruthy();
      formHelper.expectError(control, 'lockDays');
    });

    it('should have the "pattern" error [1]', () => {
      expectPatternLockError('-1');
    });

    it('should have the "pattern" error [2]', () => {
      expectPatternLockError('1.2');
    });

    it('should have valid values [1]', () => {
      expectValidLockInputs(true, 'Governance', '1');
    });

    it('should have valid values [2]', () => {
      expectValidLockInputs(false, 'Compliance', '2');
    });
  });
});

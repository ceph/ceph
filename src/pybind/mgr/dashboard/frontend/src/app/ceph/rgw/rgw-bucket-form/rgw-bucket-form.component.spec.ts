import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { FormControl, ReactiveFormsModule } from '@angular/forms';
import { Router } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';

import * as _ from 'lodash';
import { ToastrModule } from 'ngx-toastr';
import { of as observableOf } from 'rxjs';

import { configureTestBed, FormHelper, i18nProviders } from '../../../../testing/unit-test-helper';
import { RgwBucketService } from '../../../shared/api/rgw-bucket.service';
import { RgwSiteService } from '../../../shared/api/rgw-site.service';
import { RgwUserService } from '../../../shared/api/rgw-user.service';
import { NotificationType } from '../../../shared/enum/notification-type.enum';
import { NotificationService } from '../../../shared/services/notification.service';
import { SharedModule } from '../../../shared/shared.module';
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
    ],
    providers: [i18nProviders]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RgwBucketFormComponent);
    component = fixture.componentInstance;
    rgwBucketService = TestBed.get(RgwBucketService);
    rgwBucketServiceGetSpy = spyOn(rgwBucketService, 'get');
    getPlacementTargetsSpy = spyOn(TestBed.get(RgwSiteService), 'getPlacementTargets');
    enumerateSpy = spyOn(TestBed.get(RgwUserService), 'enumerate');
    formHelper = new FormHelper(component.bucketForm);
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('bucketNameValidator', () => {
    const testValidator = (name: string, valid: boolean) => {
      const validatorFn = component.bucketNameValidator();
      const ctrl = new FormControl(name);
      ctrl.markAsDirty();
      const validatorPromise = validatorFn(ctrl);
      expect(validatorPromise instanceof Promise).toBeTruthy();
      if (validatorPromise instanceof Promise) {
        validatorPromise.then((resp) => {
          if (valid) {
            expect(resp).toBe(null);
          } else {
            expect(resp instanceof Object).toBeTruthy();
            expect(resp.bucketNameInvalid).toBeTruthy();
          }
        });
      }
    };

    it('should validate empty name', () => {
      testValidator('', true);
    });

    it('bucket names cannot be formatted as IP address', () => {
      testValidator('172.10.4.51', false);
    });

    it('bucket name must be >= 3 characters long (1/2)', () => {
      testValidator('ab', false);
    });

    it('bucket name must be >= 3 characters long (2/2)', () => {
      testValidator('abc', true);
    });

    it('bucket name must be <= than 63 characters long (1/2)', () => {
      testValidator(_.repeat('a', 64), false);
    });

    it('bucket name must be <= than 63 characters long (2/2)', () => {
      testValidator(_.repeat('a', 63), true);
    });

    it('bucket names must not contain uppercase characters or underscores (1/2)', () => {
      testValidator('iAmInvalid', false);
    });

    it('bucket names must not contain uppercase characters or underscores (2/2)', () => {
      testValidator('i_am_invalid', false);
    });

    it('bucket names with invalid labels (1/3)', () => {
      testValidator('abc.1def.Ghi2', false);
    });

    it('bucket names with invalid labels (2/3)', () => {
      testValidator('abc.1-xy', false);
    });

    it('bucket names with invalid labels (3/3)', () => {
      testValidator('abc.*def', false);
    });

    it('bucket names must be a series of one or more labels and can contain lowercase letters, numbers, and hyphens (1/3)', () => {
      testValidator('xyz.abc', true);
    });

    it('bucket names must be a series of one or more labels and can contain lowercase letters, numbers, and hyphens (2/3)', () => {
      testValidator('abc.1-def', true);
    });

    it('bucket names must be a series of one or more labels and can contain lowercase letters, numbers, and hyphens (3/3)', () => {
      testValidator('abc.ghi2', true);
    });

    it('bucket names must be unique', () => {
      spyOn(rgwBucketService, 'enumerate').and.returnValue(observableOf(['abcd']));
      const validatorFn = component.bucketNameValidator();
      const ctrl = new FormControl('abcd');
      ctrl.markAsDirty();
      const validatorPromise = validatorFn(ctrl);
      expect(validatorPromise instanceof Promise).toBeTruthy();
      if (validatorPromise instanceof Promise) {
        validatorPromise.then((resp) => {
          expect(resp instanceof Object).toBeTruthy();
          expect(resp.bucketNameExists).toBeTruthy();
        });
      }
    });

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
      spyOn(TestBed.get(Router), 'navigate').and.stub();
      notificationService = TestBed.get(NotificationService);
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
        'Created Object Gateway bucket ""'
      );
    });

    it('tests update success notification', () => {
      spyOn(rgwBucketService, 'update').and.returnValue(observableOf([]));
      component.editing = true;
      component.bucketForm.markAsDirty();
      component.submit();
      expect(notificationService.show).toHaveBeenCalledWith(
        NotificationType.success,
        'Updated Object Gateway bucket "".'
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
    const setDaysAndYears = (fn: (name: string) => void) => {
      ['lock_retention_period_days', 'lock_retention_period_years'].forEach(fn);
    };

    const expectPatternLockError = (value: string) => {
      formHelper.setValue('lock_enabled', true, true);
      setDaysAndYears((name: string) => {
        formHelper.setValue(name, value);
        formHelper.expectError(name, 'pattern');
      });
    };

    const expectValidLockInputs = (enabled: boolean, mode: string, days: string, years: string) => {
      formHelper.setValue('lock_enabled', enabled);
      formHelper.setValue('lock_mode', mode);
      formHelper.setValue('lock_retention_period_days', days);
      formHelper.setValue('lock_retention_period_years', years);
      [
        'lock_enabled',
        'lock_mode',
        'lock_retention_period_days',
        'lock_retention_period_years'
      ].forEach((name) => {
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

    it('should have the "eitherDaysOrYears" error', () => {
      formHelper.setValue('lock_enabled', true);
      setDaysAndYears((name: string) => {
        const control = component.bucketForm.get(name);
        control.updateValueAndValidity();
        expect(control.value).toBe(0);
        expect(control.invalid).toBeTruthy();
        formHelper.expectError(control, 'eitherDaysOrYears');
      });
    });

    it('should have the "pattern" error [1]', () => {
      expectPatternLockError('-1');
    });

    it('should have the "pattern" error [2]', () => {
      expectPatternLockError('1.2');
    });

    it('should have valid values [1]', () => {
      expectValidLockInputs(true, 'Governance', '0', '1');
    });

    it('should have valid values [2]', () => {
      expectValidLockInputs(false, 'Compliance', '100', '0');
    });
  });
});

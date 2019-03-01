import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { FormControl, ReactiveFormsModule } from '@angular/forms';
import { Router } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';

import { ToastModule } from 'ng2-toastr';
import { of as observableOf } from 'rxjs';

import { configureTestBed, i18nProviders } from '../../../../testing/unit-test-helper';
import { RgwBucketService } from '../../../shared/api/rgw-bucket.service';
import { NotificationType } from '../../../shared/enum/notification-type.enum';
import { NotificationService } from '../../../shared/services/notification.service';
import { SharedModule } from '../../../shared/shared.module';
import { RgwBucketFormComponent } from './rgw-bucket-form.component';

describe('RgwBucketFormComponent', () => {
  let component: RgwBucketFormComponent;
  let fixture: ComponentFixture<RgwBucketFormComponent>;
  let rwgBucketService: RgwBucketService;

  configureTestBed({
    declarations: [RgwBucketFormComponent],
    imports: [
      HttpClientTestingModule,
      ReactiveFormsModule,
      RouterTestingModule,
      SharedModule,
      ToastModule.forRoot()
    ],
    providers: [i18nProviders]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RgwBucketFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
    rwgBucketService = TestBed.get(RgwBucketService);
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('bucketNameValidator', () => {
    it('should validate name (1/4)', () => {
      const validatorFn = component.bucketNameValidator();
      const ctrl = new FormControl('');
      const validatorPromise = validatorFn(ctrl);
      expect(validatorPromise instanceof Promise).toBeTruthy();
      if (validatorPromise instanceof Promise) {
        validatorPromise.then((resp) => {
          expect(resp).toBe(null);
        });
      }
    });

    it('should validate name (2/4)', () => {
      const validatorFn = component.bucketNameValidator();
      const ctrl = new FormControl('ab');
      ctrl.markAsDirty();
      const validatorPromise = validatorFn(ctrl);
      expect(validatorPromise instanceof Promise).toBeTruthy();
      if (validatorPromise instanceof Promise) {
        validatorPromise.then((resp) => {
          expect(resp.bucketNameInvalid).toBeTruthy();
        });
      }
    });

    it('should validate name (3/4)', () => {
      const validatorFn = component.bucketNameValidator();
      const ctrl = new FormControl('abc');
      ctrl.markAsDirty();
      const validatorPromise = validatorFn(ctrl);
      expect(validatorPromise instanceof Promise).toBeTruthy();
      if (validatorPromise instanceof Promise) {
        validatorPromise.then((resp) => {
          expect(resp).toBe(null);
        });
      }
    });

    it('should validate name (4/4)', () => {
      spyOn(rwgBucketService, 'enumerate').and.returnValue(observableOf(['abcd']));
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
  });

  describe('submit form', () => {
    let notificationService: NotificationService;

    beforeEach(() => {
      spyOn(TestBed.get(Router), 'navigate').and.stub();
      notificationService = TestBed.get(NotificationService);
      spyOn(notificationService, 'show');
    });

    it('tests create success notification', () => {
      spyOn(rwgBucketService, 'create').and.returnValue(observableOf([]));
      component.editing = false;
      component.bucketForm.markAsDirty();
      component.submit();
      expect(notificationService.show).toHaveBeenCalledWith(
        NotificationType.success,
        'Created Object Gateway bucket ""'
      );
    });

    it('tests update success notification', () => {
      spyOn(rwgBucketService, 'update').and.returnValue(observableOf([]));
      component.editing = true;
      component.bucketForm.markAsDirty();
      component.submit();
      expect(notificationService.show).toHaveBeenCalledWith(
        NotificationType.success,
        'Updated Object Gateway bucket ""'
      );
    });
  });
});

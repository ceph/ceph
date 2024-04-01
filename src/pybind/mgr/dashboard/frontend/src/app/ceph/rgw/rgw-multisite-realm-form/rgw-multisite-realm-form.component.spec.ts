import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { Router } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import _ from 'lodash';
import { of as observableOf } from 'rxjs';
import { ToastrModule } from 'ngx-toastr';
import { RgwRealmService } from '~/app/shared/api/rgw-realm.service';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { NotificationService } from '~/app/shared/services/notification.service';
import { SharedModule } from '~/app/shared/shared.module';

import { RgwMultisiteRealmFormComponent } from './rgw-multisite-realm-form.component';
import { configureTestBed } from '~/testing/unit-test-helper';

describe('RgwMultisiteRealmFormComponent', () => {
  let component: RgwMultisiteRealmFormComponent;
  let fixture: ComponentFixture<RgwMultisiteRealmFormComponent>;
  let rgwRealmService: RgwRealmService;

  configureTestBed({
    imports: [
      SharedModule,
      ReactiveFormsModule,
      RouterTestingModule,
      HttpClientTestingModule,
      ToastrModule.forRoot()
    ],
    providers: [NgbActiveModal],
    declarations: [RgwMultisiteRealmFormComponent]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RgwMultisiteRealmFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('submit form', () => {
    let notificationService: NotificationService;

    beforeEach(() => {
      spyOn(TestBed.inject(Router), 'navigate').and.stub();
      notificationService = TestBed.inject(NotificationService);
      spyOn(notificationService, 'show');
      rgwRealmService = TestBed.inject(RgwRealmService);
    });

    it('should validate name', () => {
      component.action = 'create';
      component.createForm();
      const control = component.multisiteRealmForm.get('realmName');
      expect(_.isFunction(control.validator)).toBeTruthy();
    });

    it('should not validate name', () => {
      component.action = 'edit';
      component.createForm();
      const control = component.multisiteRealmForm.get('realmName');
      expect(control.asyncValidator).toBeNull();
    });

    it('tests create success notification', () => {
      spyOn(rgwRealmService, 'create').and.returnValue(observableOf([]));
      component.action = 'create';
      component.multisiteRealmForm.markAsDirty();
      component.submit();
      expect(notificationService.show).toHaveBeenCalledWith(
        NotificationType.success,
        "Realm: 'null' created successfully"
      );
    });

    it('tests update success notification', () => {
      spyOn(rgwRealmService, 'update').and.returnValue(observableOf([]));
      component.action = 'edit';
      component.info = {
        data: { name: 'null' }
      };
      component.multisiteRealmForm.markAsDirty();
      component.submit();
      expect(notificationService.show).toHaveBeenCalledWith(
        NotificationType.success,
        "Realm: 'null' updated successfully"
      );
    });
  });
});

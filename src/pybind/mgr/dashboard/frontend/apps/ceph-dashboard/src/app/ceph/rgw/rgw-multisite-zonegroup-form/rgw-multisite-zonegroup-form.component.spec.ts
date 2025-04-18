import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { Router } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import _ from 'lodash';
import { ToastrModule } from 'ngx-toastr';
import { of as observableOf } from 'rxjs';
import { RgwZonegroupService } from '~/app/shared/api/rgw-zonegroup.service';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { NotificationService } from '~/app/shared/services/notification.service';
import { SharedModule } from '~/app/shared/shared.module';

import { RgwMultisiteZonegroupFormComponent } from './rgw-multisite-zonegroup-form.component';
import { configureTestBed } from '~/testing/unit-test-helper';

describe('RgwMultisiteZonegroupFormComponent', () => {
  let component: RgwMultisiteZonegroupFormComponent;
  let fixture: ComponentFixture<RgwMultisiteZonegroupFormComponent>;
  let rgwZonegroupService: RgwZonegroupService;

  configureTestBed({
    imports: [
      SharedModule,
      ReactiveFormsModule,
      RouterTestingModule,
      HttpClientTestingModule,
      ToastrModule.forRoot()
    ],
    providers: [NgbActiveModal],
    declarations: [RgwMultisiteZonegroupFormComponent]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RgwMultisiteZonegroupFormComponent);
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
      rgwZonegroupService = TestBed.inject(RgwZonegroupService);
    });

    it('should validate name', () => {
      component.action = 'create';
      component.createForm();
      const control = component.multisiteZonegroupForm.get('zonegroupName');
      expect(_.isFunction(control.validator)).toBeTruthy();
    });

    it('should not validate name', () => {
      component.action = 'edit';
      component.createForm();
      const control = component.multisiteZonegroupForm.get('zonegroupName');
      expect(control.asyncValidator).toBeNull();
    });

    it('tests create success notification', () => {
      spyOn(rgwZonegroupService, 'create').and.returnValue(observableOf([]));
      component.action = 'create';
      component.multisiteZonegroupForm.markAsDirty();
      component.multisiteZonegroupForm._get('zonegroupName').setValue('zg-1');
      component.multisiteZonegroupForm
        ._get('zonegroup_endpoints')
        .setValue('http://192.1.1.1:8004');
      component.submit();
      expect(notificationService.show).toHaveBeenCalledWith(
        NotificationType.success,
        "Zonegroup: 'zg-1' created successfully"
      );
    });

    it('tests update success notification', () => {
      spyOn(rgwZonegroupService, 'update').and.returnValue(observableOf([]));
      component.action = 'edit';
      component.info = {
        data: { name: 'zg-1', zones: ['z1'] }
      };
      component.multisiteZonegroupForm._get('zonegroupName').setValue('zg-1');
      component.multisiteZonegroupForm
        ._get('zonegroup_endpoints')
        .setValue('http://192.1.1.1:8004,http://192.12.12.12:8004');
      component.multisiteZonegroupForm.markAsDirty();
      component.submit();
      expect(notificationService.show).toHaveBeenCalledWith(
        NotificationType.success,
        "Zonegroup: 'zg-1' updated successfully"
      );
    });
  });
});

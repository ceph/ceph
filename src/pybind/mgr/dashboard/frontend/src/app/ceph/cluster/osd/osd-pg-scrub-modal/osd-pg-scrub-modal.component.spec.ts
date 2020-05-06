import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { Router } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';

import { BsModalRef, ModalModule } from 'ngx-bootstrap/modal';
import { ToastrModule } from 'ngx-toastr';
import { of as observableOf } from 'rxjs';

import { configureTestBed, i18nProviders } from '../../../../../testing/unit-test-helper';
import { ConfigurationService } from '../../../../shared/api/configuration.service';
import { NotificationType } from '../../../../shared/enum/notification-type.enum';
import { NotificationService } from '../../../../shared/services/notification.service';
import { SharedModule } from '../../../../shared/shared.module';
import { OsdPgScrubModalComponent } from './osd-pg-scrub-modal.component';

describe('OsdPgScrubModalComponent', () => {
  let component: OsdPgScrubModalComponent;
  let fixture: ComponentFixture<OsdPgScrubModalComponent>;
  let configurationService: ConfigurationService;

  configureTestBed({
    imports: [
      HttpClientTestingModule,
      ModalModule.forRoot(),
      ReactiveFormsModule,
      RouterTestingModule,
      SharedModule,
      ToastrModule.forRoot()
    ],
    declarations: [OsdPgScrubModalComponent],
    providers: [BsModalRef, i18nProviders]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(OsdPgScrubModalComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
    configurationService = TestBed.inject(ConfigurationService);
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('submitAction', () => {
    let notificationService: NotificationService;

    beforeEach(() => {
      spyOn(TestBed.inject(Router), 'navigate').and.stub();
      notificationService = TestBed.inject(NotificationService);
      spyOn(notificationService, 'show');
    });

    it('test create success notification', () => {
      spyOn(configurationService, 'bulkCreate').and.returnValue(observableOf([]));
      component.submitAction();
      expect(notificationService.show).toHaveBeenCalledWith(
        NotificationType.success,
        'Updated PG scrub options'
      );
    });
  });
});

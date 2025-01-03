import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import _ from 'lodash';
import { ToastrModule } from 'ngx-toastr';

import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { NotificationService } from '~/app/shared/services/notification.service';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { OsdFlagsModalComponent } from './osd-flags-modal.component';

function getFlagsArray(component: OsdFlagsModalComponent) {
  const allFlags = _.cloneDeep(component.allFlags);
  allFlags['purged_snapdirs'].value = true;
  allFlags['pause'].value = true;
  return _.toArray(allFlags);
}

describe('OsdFlagsModalComponent', () => {
  let component: OsdFlagsModalComponent;
  let fixture: ComponentFixture<OsdFlagsModalComponent>;
  let httpTesting: HttpTestingController;

  configureTestBed({
    imports: [
      ReactiveFormsModule,
      SharedModule,
      HttpClientTestingModule,
      RouterTestingModule,
      ToastrModule.forRoot()
    ],
    declarations: [OsdFlagsModalComponent],
    providers: [NgbActiveModal]
  });

  beforeEach(() => {
    httpTesting = TestBed.inject(HttpTestingController);
    fixture = TestBed.createComponent(OsdFlagsModalComponent);
    component = fixture.componentInstance;
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should finish running ngOnInit', () => {
    fixture.detectChanges();

    const flags = getFlagsArray(component);

    const req = httpTesting.expectOne('api/osd/flags');
    req.flush(['purged_snapdirs', 'pause', 'foo']);

    expect(component.flags).toEqual(flags);
    expect(component.unknownFlags).toEqual(['foo']);
  });

  describe('test submitAction', function () {
    let notificationType: NotificationType;
    let notificationService: NotificationService;
    let bsModalRef: NgbActiveModal;

    beforeEach(() => {
      notificationService = TestBed.inject(NotificationService);
      spyOn(notificationService, 'show').and.callFake((type) => {
        notificationType = type;
      });

      bsModalRef = TestBed.inject(NgbActiveModal);
      spyOn(bsModalRef, 'close').and.callThrough();
      component.unknownFlags = ['foo'];
    });

    it('should run submitAction', () => {
      component.flags = getFlagsArray(component);
      component.submitAction();
      const req = httpTesting.expectOne('api/osd/flags');
      req.flush(['purged_snapdirs', 'pause', 'foo']);
      expect(req.request.body).toEqual({ flags: ['pause', 'purged_snapdirs', 'foo'] });

      expect(notificationType).toBe(NotificationType.success);
      expect(component.activeModal.close).toHaveBeenCalledTimes(1);
    });

    it('should hide modal if request fails', () => {
      component.flags = [];
      component.submitAction();
      const req = httpTesting.expectOne('api/osd/flags');
      req.flush([], { status: 500, statusText: 'failure' });

      expect(notificationService.show).toHaveBeenCalledTimes(0);
      expect(component.activeModal.close).toHaveBeenCalledTimes(1);
    });
  });
});

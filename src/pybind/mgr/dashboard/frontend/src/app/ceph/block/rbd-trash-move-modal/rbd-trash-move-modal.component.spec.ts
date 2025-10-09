import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbPopoverModule } from '@ng-bootstrap/ng-bootstrap';
import moment from 'moment';
import { ToastrModule } from 'ngx-toastr';

import { NotificationService } from '~/app/shared/services/notification.service';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { RbdTrashMoveModalComponent } from './rbd-trash-move-modal.component';
import {
  CheckboxModule,
  DatePickerModule,
  ModalModule,
  TimePickerModule
} from 'carbon-components-angular';
import { DateTimePickerComponent } from '~/app/shared/components/date-time-picker/date-time-picker.component';

describe('RbdTrashMoveModalComponent', () => {
  let component: RbdTrashMoveModalComponent;
  let fixture: ComponentFixture<RbdTrashMoveModalComponent>;
  let httpTesting: HttpTestingController;

  configureTestBed({
    imports: [
      ReactiveFormsModule,
      HttpClientTestingModule,
      RouterTestingModule,
      SharedModule,
      ToastrModule.forRoot(),
      NgbPopoverModule,
      ModalModule,
      CheckboxModule,
      DatePickerModule,
      TimePickerModule
    ],
    declarations: [RbdTrashMoveModalComponent, DateTimePickerComponent],
    providers: [
      { provide: 'poolName', useValue: 'foo' },
      { provide: 'imageName', useValue: 'bar' },
      { provide: 'namespace', useValue: '' },
      { provide: 'hasSnapshots', useValue: false }
    ]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RbdTrashMoveModalComponent);
    component = fixture.componentInstance;
    httpTesting = TestBed.inject(HttpTestingController);

    component.poolName = 'foo';
    component.imageName = 'bar';

    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
    expect(component.moveForm).toBeDefined();
  });

  it('should finish running ngOnInit', () => {
    expect(component.pattern).toEqual('foo/bar');
  });

  describe('should call moveImage', () => {
    let notificationService: NotificationService;

    beforeEach(() => {
      notificationService = TestBed.inject(NotificationService);
      spyOn(notificationService, 'show').and.stub();
      spyOn(component, 'closeModal').and.callThrough();
    });

    afterEach(() => {
      expect(notificationService.show).toHaveBeenCalledTimes(1);
      expect(component.closeModal).toHaveBeenCalledTimes(1);
    });

    it('with normal delay', () => {
      component.moveImage();
      const req = httpTesting.expectOne('api/block/image/foo%2Fbar/move_trash');
      req.flush(null);
      expect(req.request.body).toEqual({ delay: 0 });
    });

    it('with delay < 0', () => {
      const oldDate = moment().subtract(24, 'hour').toDate();
      component.moveForm.patchValue({ expiresAt: oldDate });

      component.moveImage();
      const req = httpTesting.expectOne('api/block/image/foo%2Fbar/move_trash');
      req.flush(null);
      expect(req.request.body).toEqual({ delay: 0 });
    });

    it('with delay < 0', () => {
      const oldDate = moment().add(24, 'hour').toISOString();
      component.moveForm.patchValue({ expiresAt: oldDate });

      component.moveImage();
      const req = httpTesting.expectOne('api/block/image/foo%2Fbar/move_trash');
      req.flush(null);
      expect(req.request.body.delay).toBeGreaterThan(56666);
    });
  });
});

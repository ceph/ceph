import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbActiveModal, NgbPopoverModule } from '@ng-bootstrap/ng-bootstrap';
import * as moment from 'moment';
import { ToastrModule } from 'ngx-toastr';

import { configureTestBed } from '../../../../testing/unit-test-helper';
import { NotificationService } from '../../../shared/services/notification.service';
import { SharedModule } from '../../../shared/shared.module';
import { RbdTrashMoveModalComponent } from './rbd-trash-move-modal.component';

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
      NgbPopoverModule
    ],
    declarations: [RbdTrashMoveModalComponent],
    providers: [NgbActiveModal]
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
      spyOn(component.activeModal, 'close').and.callThrough();
    });

    afterEach(() => {
      expect(notificationService.show).toHaveBeenCalledTimes(1);
      expect(component.activeModal.close).toHaveBeenCalledTimes(1);
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
      expect(req.request.body.delay).toBeGreaterThan(86390);
    });
  });
});

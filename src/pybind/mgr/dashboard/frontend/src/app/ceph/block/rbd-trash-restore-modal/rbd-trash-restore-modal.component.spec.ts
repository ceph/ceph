import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';

import { ToastModule } from 'ng2-toastr';
import { BsModalRef } from 'ngx-bootstrap';

import { configureTestBed } from '../../../../testing/unit-test-helper';
import { NotificationService } from '../../../shared/services/notification.service';
import { SharedModule } from '../../../shared/shared.module';
import { RbdTrashRestoreModalComponent } from './rbd-trash-restore-modal.component';

describe('RbdTrashRestoreModalComponent', () => {
  let component: RbdTrashRestoreModalComponent;
  let fixture: ComponentFixture<RbdTrashRestoreModalComponent>;

  configureTestBed({
    declarations: [RbdTrashRestoreModalComponent],
    imports: [
      ReactiveFormsModule,
      HttpClientTestingModule,
      ToastModule.forRoot(),
      SharedModule,
      RouterTestingModule
    ],
    providers: [BsModalRef]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RbdTrashRestoreModalComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('should call restore', () => {
    let httpTesting: HttpTestingController;
    let notificationService: NotificationService;
    let modalRef: BsModalRef;
    let req;

    beforeEach(() => {
      httpTesting = TestBed.get(HttpTestingController);
      notificationService = TestBed.get(NotificationService);
      modalRef = TestBed.get(BsModalRef);

      component.poolName = 'foo';
      component.imageId = 'bar';

      spyOn(modalRef, 'hide').and.stub();
      spyOn(component.restoreForm, 'setErrors').and.stub();
      spyOn(notificationService, 'show').and.stub();

      component.restore();

      req = httpTesting.expectOne('api/block/image/trash/foo/bar/restore');
    });

    it('with success', () => {
      req.flush(null);
      expect(component.restoreForm.setErrors).toHaveBeenCalledTimes(0);
      expect(component.modalRef.hide).toHaveBeenCalledTimes(1);
    });

    it('with failure', () => {
      req.flush(null, { status: 500, statusText: 'failure' });
      expect(component.restoreForm.setErrors).toHaveBeenCalledTimes(1);
      expect(component.modalRef.hide).toHaveBeenCalledTimes(0);
    });
  });
});

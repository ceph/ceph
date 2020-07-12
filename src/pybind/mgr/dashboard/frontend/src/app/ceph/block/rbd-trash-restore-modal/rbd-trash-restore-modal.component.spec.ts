import {
  HttpClientTestingModule,
  HttpTestingController,
  TestRequest
} from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { ToastrModule } from 'ngx-toastr';

import { configureTestBed, i18nProviders } from '../../../../testing/unit-test-helper';
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
      ToastrModule.forRoot(),
      SharedModule,
      RouterTestingModule
    ],
    providers: [NgbActiveModal, i18nProviders]
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
    let activeModal: NgbActiveModal;
    let req: TestRequest;

    beforeEach(() => {
      httpTesting = TestBed.inject(HttpTestingController);
      notificationService = TestBed.inject(NotificationService);
      activeModal = TestBed.inject(NgbActiveModal);

      component.poolName = 'foo';
      component.imageName = 'bar';
      component.imageId = '113cb6963793';
      component.ngOnInit();

      spyOn(activeModal, 'close').and.stub();
      spyOn(component.restoreForm, 'setErrors').and.stub();
      spyOn(notificationService, 'show').and.stub();

      component.restore();

      req = httpTesting.expectOne('api/block/image/trash/foo%2F113cb6963793/restore');
    });

    it('with success', () => {
      req.flush(null);
      expect(component.restoreForm.setErrors).toHaveBeenCalledTimes(0);
      expect(component.activeModal.close).toHaveBeenCalledTimes(1);
    });

    it('with failure', () => {
      req.flush(null, { status: 500, statusText: 'failure' });
      expect(component.restoreForm.setErrors).toHaveBeenCalledTimes(1);
      expect(component.activeModal.close).toHaveBeenCalledTimes(0);
    });
  });
});

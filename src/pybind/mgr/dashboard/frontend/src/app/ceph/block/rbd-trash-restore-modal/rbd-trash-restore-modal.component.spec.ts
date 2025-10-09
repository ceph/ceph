import {
  HttpClientTestingModule,
  HttpTestingController,
  TestRequest
} from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';

import { ToastrModule } from 'ngx-toastr';

import { NotificationService } from '~/app/shared/services/notification.service';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { RbdTrashRestoreModalComponent } from './rbd-trash-restore-modal.component';
import { InputModule, ModalModule } from 'carbon-components-angular';

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
      RouterTestingModule,
      InputModule,
      ModalModule
    ],
    providers: [
      { provide: 'poolName', useValue: 'foo' },
      { provide: 'namespace', useValue: '' },
      { provide: 'imageName', useValue: 'bar' },
      { provide: 'imageId', useValue: '' }
    ]
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
    let req: TestRequest;

    beforeEach(() => {
      httpTesting = TestBed.inject(HttpTestingController);
      notificationService = TestBed.inject(NotificationService);

      component.poolName = 'foo';
      component.imageName = 'bar';
      component.imageId = '113cb6963793';
      component.ngOnInit();

      spyOn(component, 'closeModal').and.stub();
      spyOn(component.restoreForm, 'setErrors').and.stub();
      spyOn(notificationService, 'show').and.stub();

      component.restore();

      req = httpTesting.expectOne('api/block/image/trash/foo%2F113cb6963793/restore');
    });

    it('with success', () => {
      req.flush(null);
      expect(component.restoreForm.setErrors).toHaveBeenCalledTimes(0);
      expect(component.closeModal).toHaveBeenCalledTimes(1);
    });

    it('with failure', () => {
      req.flush(null, { status: 500, statusText: 'failure' });
      expect(component.restoreForm.setErrors).toHaveBeenCalledTimes(1);
      expect(component.closeModal).toHaveBeenCalledTimes(0);
    });
  });
});

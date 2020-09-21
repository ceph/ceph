import {
  HttpClientTestingModule,
  HttpTestingController,
  TestRequest
} from '@angular/common/http/testing';
import { ComponentFixture, fakeAsync, TestBed, tick } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { ToastrModule } from 'ngx-toastr';

import { configureTestBed } from '../../../../testing/unit-test-helper';
import { Permission } from '../../../shared/models/permissions';
import { NotificationService } from '../../../shared/services/notification.service';
import { SharedModule } from '../../../shared/shared.module';
import { RbdTrashPurgeModalComponent } from './rbd-trash-purge-modal.component';

describe('RbdTrashPurgeModalComponent', () => {
  let component: RbdTrashPurgeModalComponent;
  let fixture: ComponentFixture<RbdTrashPurgeModalComponent>;
  let httpTesting: HttpTestingController;

  configureTestBed({
    imports: [
      HttpClientTestingModule,
      ReactiveFormsModule,
      SharedModule,
      ToastrModule.forRoot(),
      RouterTestingModule
    ],
    declarations: [RbdTrashPurgeModalComponent],
    providers: [NgbActiveModal]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RbdTrashPurgeModalComponent);
    httpTesting = TestBed.inject(HttpTestingController);
    component = fixture.componentInstance;
  });

  it('should create', () => {
    fixture.detectChanges();
    expect(component).toBeTruthy();
  });

  it('should finish ngOnInit', fakeAsync(() => {
    component.poolPermission = new Permission(['read', 'create', 'update', 'delete']);
    fixture.detectChanges();
    const req = httpTesting.expectOne('api/pool?attrs=pool_name,application_metadata');
    req.flush([
      {
        application_metadata: ['foo'],
        pool_name: 'bar'
      },
      {
        application_metadata: ['rbd'],
        pool_name: 'baz'
      }
    ]);
    tick();
    expect(component.pools).toEqual(['baz']);
    expect(component.purgeForm).toBeTruthy();
  }));

  it('should call ngOnInit without pool permissions', () => {
    component.poolPermission = new Permission([]);
    component.ngOnInit();
    httpTesting.verify();
  });

  describe('should call purge', () => {
    let notificationService: NotificationService;
    let activeModal: NgbActiveModal;
    let req: TestRequest;

    beforeEach(() => {
      fixture.detectChanges();
      notificationService = TestBed.inject(NotificationService);
      activeModal = TestBed.inject(NgbActiveModal);

      component.purgeForm.patchValue({ poolName: 'foo' });

      spyOn(activeModal, 'close').and.stub();
      spyOn(component.purgeForm, 'setErrors').and.stub();
      spyOn(notificationService, 'show').and.stub();

      component.purge();

      req = httpTesting.expectOne('api/block/image/trash/purge/?pool_name=foo');
    });

    it('with success', () => {
      req.flush(null);
      expect(component.purgeForm.setErrors).toHaveBeenCalledTimes(0);
      expect(component.activeModal.close).toHaveBeenCalledTimes(1);
    });

    it('with failure', () => {
      req.flush(null, { status: 500, statusText: 'failure' });
      expect(component.purgeForm.setErrors).toHaveBeenCalledTimes(1);
      expect(component.activeModal.close).toHaveBeenCalledTimes(0);
    });
  });
});

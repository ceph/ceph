import { HttpClientTestingModule } from '@angular/common/http/testing';
import { async, ComponentFixture, fakeAsync, TestBed, tick } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { ToastModule } from 'ng2-toastr';
import { BsModalRef, ModalModule } from 'ngx-bootstrap';
import { Observable } from 'rxjs/Observable';

import { ApiModule } from '../../../shared/api/api.module';
import { RbdService } from '../../../shared/api/rbd.service';
import { ComponentsModule } from '../../../shared/components/components.module';
import { DataTableModule } from '../../../shared/datatable/datatable.module';
import { PipesModule } from '../../../shared/pipes/pipes.module';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';
import { NotificationService } from '../../../shared/services/notification.service';
import { ServicesModule } from '../../../shared/services/services.module';
import { RbdSnapshotListComponent } from './rbd-snapshot-list.component';

describe('RbdSnapshotListComponent', () => {
  let component: RbdSnapshotListComponent;
  let fixture: ComponentFixture<RbdSnapshotListComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ RbdSnapshotListComponent ],
      imports: [
        DataTableModule,
        ComponentsModule,
        ModalModule.forRoot(),
        ToastModule.forRoot(),
        ServicesModule,
        ApiModule,
        HttpClientTestingModule,
        RouterTestingModule,
        PipesModule
      ],
      providers: [ AuthStorageService ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(RbdSnapshotListComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('api delete request', () => {
    let called;
    let rbdService: RbdService;
    let notificationService: NotificationService;

    beforeEach(() => {
      called = false;
      rbdService = new RbdService(null);
      notificationService = new NotificationService(null, null);
      component = new RbdSnapshotListComponent(null, null, null, rbdService, null, null,
                                               notificationService);
      spyOn(rbdService, 'deleteSnapshot').and.returnValue(Observable.throw({status: 500}));
      spyOn(notificationService, 'notifyTask').and.stub();
      component.modalRef = new BsModalRef();
      component.modalRef.content = {
        stopLoadingSpinner: () => called = true
      };
    });

    it('should call stopLoadingSpinner if the request fails', <any>fakeAsync(() => {
      expect(called).toBe(false);
      component._asyncTask('deleteSnapshot', 'rbd/snap/delete', 'someName');
      tick(500);
      expect(called).toBe(true);
    }));
  });
});

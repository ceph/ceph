import { HttpClientTestingModule } from '@angular/common/http/testing';
import { async, ComponentFixture, fakeAsync, TestBed, tick } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { ToastModule } from 'ng2-toastr';
import {
  AlertModule,
  BsDropdownModule, BsModalRef,
  ModalModule,
  TabsModule,
  TooltipModule
} from 'ngx-bootstrap';
import { Observable } from 'rxjs/Observable';

import { RbdService } from '../../../shared/api/rbd.service';
import { ComponentsModule } from '../../../shared/components/components.module';
import { NotificationService } from '../../../shared/services/notification.service';
import { SharedModule } from '../../../shared/shared.module';
import { RbdDetailsComponent } from '../rbd-details/rbd-details.component';
import { RbdSnapshotListComponent } from '../rbd-snapshot-list/rbd-snapshot-list.component';
import { RbdListComponent } from './rbd-list.component';

describe('RbdListComponent', () => {
  let component: RbdListComponent;
  let fixture: ComponentFixture<RbdListComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      imports: [
        SharedModule,
        BsDropdownModule.forRoot(),
        TabsModule.forRoot(),
        ModalModule.forRoot(),
        TooltipModule.forRoot(),
        ToastModule.forRoot(),
        AlertModule.forRoot(),
        ComponentsModule,
        RouterTestingModule,
        HttpClientTestingModule
      ],
      declarations: [ RbdListComponent, RbdDetailsComponent, RbdSnapshotListComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(RbdListComponent);
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
      component = new RbdListComponent(rbdService, null, null, null, null, notificationService,
                                       null, null);
      spyOn(rbdService, 'delete').and.returnValue(Observable.throw({status: 500}));
      spyOn(notificationService, 'notifyTask').and.stub();
      component.modalRef = new BsModalRef();
      component.modalRef.content = {
        stopLoadingSpinner: () => called = true
      };
    });

    it('should make sure that if the deletion fails stopLoadingSpinner is called',
        <any>fakeAsync(() => {
          expect(called).toBe(false);
          component.deleteRbd('sth', 'test');
          tick(500);
          expect(called).toBe(true);
        }));
  });
});

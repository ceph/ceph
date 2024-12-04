import { ComponentFixture, TestBed, fakeAsync, tick } from '@angular/core/testing';

import { NvmeofListenersListComponent } from './nvmeof-listeners-list.component';
import { HttpClientModule } from '@angular/common/http';
import { RouterTestingModule } from '@angular/router/testing';
import { SharedModule } from '~/app/shared/shared.module';
import { NvmeofService } from '~/app/shared/api/nvmeof.service';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { ModalService } from '~/app/shared/services/modal.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { of } from 'rxjs';

const mockListeners = [
  {
    host_name: 'ceph-node-02',
    trtype: 'TCP',
    traddr: '192.168.100.102',
    adrfam: 0,
    trsvcid: 4421
  }
];

class MockNvmeOfService {
  listListeners() {
    return of(mockListeners);
  }
}

class MockAuthStorageService {
  getPermissions() {
    return { nvmeof: {} };
  }
}

class MockModalService {}

class MockTaskWrapperService {}

describe('NvmeofListenersListComponent', () => {
  let component: NvmeofListenersListComponent;
  let fixture: ComponentFixture<NvmeofListenersListComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [NvmeofListenersListComponent],
      imports: [HttpClientModule, RouterTestingModule, SharedModule],
      providers: [
        { provide: NvmeofService, useClass: MockNvmeOfService },
        { provide: AuthStorageService, useClass: MockAuthStorageService },
        { provide: ModalService, useClass: MockModalService },
        { provide: TaskWrapperService, useClass: MockTaskWrapperService }
      ]
    }).compileComponents();

    fixture = TestBed.createComponent(NvmeofListenersListComponent);
    component = fixture.componentInstance;
    component.ngOnInit();
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should retrieve listeners', fakeAsync(() => {
    component.listListeners();
    tick();
    expect(component.listeners).toEqual(mockListeners);
  }));
});

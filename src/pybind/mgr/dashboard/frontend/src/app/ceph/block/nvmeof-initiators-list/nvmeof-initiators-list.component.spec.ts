import { ComponentFixture, TestBed, fakeAsync, tick } from '@angular/core/testing';
import { HttpClientModule } from '@angular/common/http';
import { RouterTestingModule } from '@angular/router/testing';

import { of } from 'rxjs';

import { SharedModule } from '~/app/shared/shared.module';
import { NvmeofService } from '~/app/shared/api/nvmeof.service';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { ModalService } from '~/app/shared/services/modal.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';

import { NvmeofInitiatorsListComponent } from './nvmeof-initiators-list.component';

const mockInitiators = [
  {
    nqn: '*',
    use_dhchap: ''
  }
];

const mockSubsystem = {
  nqn: 'nqn.2016-06.io.spdk:cnode1',
  serial_number: '12345',
  has_dhchap_key: false
};

class MockNvmeOfService {
  getInitiators() {
    return of(mockInitiators);
  }
  getSubsystem() {
    return of(mockSubsystem);
  }
}

class MockAuthStorageService {
  getPermissions() {
    return { nvmeof: { read: true, create: true, delete: true } };
  }
}

class MockModalService {}

class MockTaskWrapperService {}

describe('NvmeofInitiatorsListComponent', () => {
  let component: NvmeofInitiatorsListComponent;
  let fixture: ComponentFixture<NvmeofInitiatorsListComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [NvmeofInitiatorsListComponent],
      imports: [HttpClientModule, RouterTestingModule, SharedModule],
      providers: [
        { provide: NvmeofService, useClass: MockNvmeOfService },
        { provide: AuthStorageService, useClass: MockAuthStorageService },
        { provide: ModalService, useClass: MockModalService },
        { provide: TaskWrapperService, useClass: MockTaskWrapperService }
      ]
    }).compileComponents();

    fixture = TestBed.createComponent(NvmeofInitiatorsListComponent);
    component = fixture.componentInstance;
    component.subsystemNQN = 'nqn.2016-06.io.spdk:cnode1';
    component.group = 'group1';
    component.ngOnInit();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should retrieve initiators and subsystem', fakeAsync(() => {
    component.listInitiators();
    component.getSubsystem();
    tick();
    expect(component.initiators).toEqual(mockInitiators);
    expect(component.subsystem).toEqual(mockSubsystem);
    expect(component.authStatus).toBe('No authentication');
  }));

  it('should update authStatus when initiator has dhchap_key', fakeAsync(() => {
    const initiatorsWithKey = [{ nqn: 'nqn1', use_dhchap: 'key1' }];
    spyOn(TestBed.inject(NvmeofService), 'getInitiators').and.returnValue(of(initiatorsWithKey));
    component.listInitiators();
    tick();
    expect(component.authStatus).toBe('Unidirectional');
  }));

  it('should update authStatus when subsystem has psk', fakeAsync(() => {
    const subsystemWithPsk = { ...mockSubsystem, has_dhchap_key: true };
    component.initiators = [{ nqn: 'nqn1', use_dhchap: 'key1' }];
    spyOn(TestBed.inject(NvmeofService), 'getSubsystem').and.returnValue(of(subsystemWithPsk));
    component.getSubsystem();
    tick();
    expect(component.authStatus).toBe('Bi-directional');
  }));
});

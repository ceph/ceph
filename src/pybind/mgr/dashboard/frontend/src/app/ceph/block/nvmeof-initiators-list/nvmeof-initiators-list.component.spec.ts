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
    nqn: '*'
  }
];

class MockNvmeOfService {
  getInitiators() {
    return of(mockInitiators);
  }
}

class MockAuthStorageService {
  getPermissions() {
    return { nvmeof: {} };
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
    component.ngOnInit();
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should retrieve initiators', fakeAsync(() => {
    component.listInitiators();
    tick();
    expect(component.initiators).toEqual(mockInitiators);
  }));
});

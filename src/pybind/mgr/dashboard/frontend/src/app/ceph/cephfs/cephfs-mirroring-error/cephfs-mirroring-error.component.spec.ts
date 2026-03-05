import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { Router } from '@angular/router';
import { of } from 'rxjs';

import { CephfsMirroringErrorComponent } from './cephfs-mirroring-error.component';
import { MgrModuleService } from '~/app/shared/api/mgr-module.service';
import { SharedModule } from '~/app/shared/shared.module';
import { RouterTestingModule } from '@angular/router/testing';

describe('CephfsMirroringErrorComponent', () => {
  let component: CephfsMirroringErrorComponent;
  let fixture: ComponentFixture<CephfsMirroringErrorComponent>;

  const routerMock = {
    events: of({}),
    onSameUrlNavigation: 'reload' as const,
    navigate: jest.fn()
  };

  const mgrModuleServiceMock = {
    updateModuleState: jest.fn(),
    updateCompleted$: { subscribe: jest.fn().mockReturnValue({ unsubscribe: jest.fn() }) }
  };

  beforeEach(async () => {
    jest.clearAllMocks();

    await TestBed.configureTestingModule({
      declarations: [CephfsMirroringErrorComponent],
      imports: [SharedModule, RouterTestingModule],
      providers: [
        { provide: Router, useValue: routerMock },
        { provide: MgrModuleService, useValue: mgrModuleServiceMock }
      ],
      schemas: [NO_ERRORS_SCHEMA]
    }).compileComponents();

    fixture = TestBed.createComponent(CephfsMirroringErrorComponent);
    component = fixture.componentInstance;
  });

  it('should create', () => {
    fixture.detectChanges();
    expect(component).toBeTruthy();
  });

  it('should call mgrModuleService.updateModuleState when enableModule is called', () => {
    fixture.detectChanges();
    component.enableModule();
    expect(mgrModuleServiceMock.updateModuleState).toHaveBeenCalledWith(
      'mirroring',
      false,
      null,
      'cephfs/mirroring',
      expect.any(String),
      false,
      expect.any(String)
    );
  });
});

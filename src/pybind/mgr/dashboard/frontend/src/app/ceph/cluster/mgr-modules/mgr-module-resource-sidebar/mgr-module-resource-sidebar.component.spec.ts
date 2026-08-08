import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ActivatedRoute, ParamMap, convertToParamMap } from '@angular/router';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { Subject } from 'rxjs';

import { SharedModule } from '~/app/shared/shared.module';
import { MgrModuleResourceStateService } from '~/app/shared/services/mgr-module-resource-state.service';
import { configureTestBed } from '~/testing/unit-test-helper';
import { MgrModuleResourceSidebarComponent } from './mgr-module-resource-sidebar.component';

describe('MgrModuleResourceSidebarComponent', () => {
  let component: MgrModuleResourceSidebarComponent;
  let fixture: ComponentFixture<MgrModuleResourceSidebarComponent>;
  const stateServiceMock = {
    load: jest.fn()
  };

  const paramMapSubject = new Subject<ParamMap>();

  configureTestBed({
    declarations: [MgrModuleResourceSidebarComponent],
    imports: [HttpClientTestingModule, SharedModule],
    providers: [
      {
        provide: ActivatedRoute,
        useValue: {
          paramMap: paramMapSubject.asObservable()
        }
      }
    ],
    schemas: [NO_ERRORS_SCHEMA]
  });

  beforeEach(() => {
    TestBed.overrideComponent(MgrModuleResourceSidebarComponent, {
      set: {
        providers: [{ provide: MgrModuleResourceStateService, useValue: stateServiceMock }]
      }
    });

    fixture = TestBed.createComponent(MgrModuleResourceSidebarComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  afterEach(() => {
    stateServiceMock.load.mockReset();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should build overview sidebar item from route param', () => {
    paramMapSubject.next(convertToParamMap({ name: 'dashboard' }));

    expect(component.moduleNameRoute).toBe('dashboard');
    expect(component.moduleName).toBe('dashboard');
    expect(component.sidebarItems.length).toBe(1);
    expect(component.sidebarItems[0].route).toEqual(['/mgr-modules', 'dashboard', 'overview']);
    expect(stateServiceMock.load).toHaveBeenCalledWith('dashboard');
  });
});

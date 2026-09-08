import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ActivatedRoute } from '@angular/router';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ReplaySubject } from 'rxjs';

import { MgrModuleResourcePageComponent } from './mgr-module-resource-page.component';
import { MgrModuleResourceStateService } from '~/app/shared/services/mgr-module-resource-state.service';

describe('MgrModuleResourcePageComponent', () => {
  let component: MgrModuleResourcePageComponent;
  let fixture: ComponentFixture<MgrModuleResourcePageComponent>;
  let stateSubject: ReplaySubject<any>;

  beforeEach(async () => {
    stateSubject = new ReplaySubject<any>(1);
    const stateServiceMock = {
      state$: stateSubject.asObservable()
    };

    const activatedRouteMock = {
      snapshot: { data: { section: 'overview' } }
    };

    await TestBed.configureTestingModule({
      declarations: [MgrModuleResourcePageComponent],
      providers: [
        { provide: ActivatedRoute, useValue: activatedRouteMock },
        { provide: MgrModuleResourceStateService, useValue: stateServiceMock }
      ],
      schemas: [NO_ERRORS_SCHEMA]
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(MgrModuleResourcePageComponent);
    component = fixture.componentInstance;
    stateSubject.next({
      moduleNameRoute: 'dashboard',
      moduleName: 'dashboard',
      moduleInfo: {
        name: 'dashboard',
        enabled: true,
        always_on: true,
        options: {}
      },
      moduleConfig: {
        server_port: 8443,
        ssl: true
      }
    });
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should build overview fields from shared module state', () => {
    const labels = component.overviewFields.map((field) => field.label);

    expect(labels).toContain('Name');
    expect(labels).toContain('Enabled');
    expect(labels).toContain('Always-On');
    expect(labels).toContain('Server port');
    expect(labels).toContain('Ssl');
  });

  it('should set notFound when shared state has no module', () => {
    stateSubject.next(null);

    expect(component.notFound).toBe(true);
    expect(component.overviewFields).toEqual([]);
  });
});

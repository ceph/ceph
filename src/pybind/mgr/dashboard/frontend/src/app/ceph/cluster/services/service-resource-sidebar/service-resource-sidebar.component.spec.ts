import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ActivatedRoute, ParamMap, convertToParamMap } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';
import { BehaviorSubject, of } from 'rxjs';
import { NO_ERRORS_SCHEMA } from '@angular/core';

import { ServiceResourceSidebarComponent } from './service-resource-sidebar.component';
import { ServiceResourceStateService } from '~/app/shared/services/service-resource-state.service';
import { configureTestBed } from '~/testing/unit-test-helper';

describe('ServiceResourceSidebarComponent', () => {
  let component: ServiceResourceSidebarComponent;
  let fixture: ComponentFixture<ServiceResourceSidebarComponent>;
  const paramMapSubject = new BehaviorSubject<ParamMap>(
    convertToParamMap({ service_name: 'test-service' })
  );

  const serviceResourceStateServiceMock = {
    service$: of({
      service_name: 'test-service',
      certificate: { has_certificate: true }
    }),
    load: jest.fn()
  };

  configureTestBed({
    imports: [RouterTestingModule],
    declarations: [ServiceResourceSidebarComponent],
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
    TestBed.overrideProvider(ServiceResourceStateService, {
      useValue: serviceResourceStateServiceMock
    });

    fixture = TestBed.createComponent(ServiceResourceSidebarComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should build sidebar items correctly', () => {
    expect(component.sidebarItems.length).toBeGreaterThan(0);
    expect(component.serviceName).toBe('test-service');
  });
});

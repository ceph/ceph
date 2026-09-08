import { HttpClientTestingModule } from '@angular/common/http/testing';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ActivatedRoute } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';
import { BehaviorSubject, of } from 'rxjs';

import { ServiceResourceStateService } from '~/app/shared/services/service-resource-state.service';
import { configureTestBed } from '~/testing/unit-test-helper';
import { ServiceResourcePageComponent } from './service-resource-page.component';

import { RelativeDatePipe } from '~/app/shared/pipes/relative-date.pipe';

describe('ServiceResourcePageComponent', () => {
  let component: ServiceResourcePageComponent;
  let fixture: ComponentFixture<ServiceResourcePageComponent>;
  let serviceSubject$: BehaviorSubject<any>;

  const serviceResourceStateServiceMock = {
    service$: of(null)
  };

  beforeEach(() => {
    serviceSubject$ = new BehaviorSubject({
      service_name: 'mgr',
      service_type: 'mgr',
      status: { running: 2, size: 2, last_refresh: '2025-01-01T00:00:00Z' },
      certificate: { has_certificate: false, requires_certificate: false }
    });
    serviceResourceStateServiceMock.service$ = serviceSubject$.asObservable();
  });

  configureTestBed({
    imports: [HttpClientTestingModule, RouterTestingModule],
    declarations: [ServiceResourcePageComponent],
    providers: [
      {
        provide: ActivatedRoute,
        useValue: {
          snapshot: { data: { section: 'overview' } }
        }
      },
      { provide: ServiceResourceStateService, useValue: serviceResourceStateServiceMock },
      { provide: RelativeDatePipe, useValue: { transform: () => '2 days ago' } }
    ],
    schemas: [NO_ERRORS_SCHEMA]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ServiceResourcePageComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should build overview fields from selected service', () => {
    const labels = component.overviewFields.map((field) => field.label);
    expect(labels).toContain('Service');
    expect(labels).toContain('Placement');
    expect(labels).toContain('Running');
    expect(labels).toContain('Last Refreshed');
    expect(labels).toContain('Ports');
    expect(labels).toContain('Certificate Status');
  });
});

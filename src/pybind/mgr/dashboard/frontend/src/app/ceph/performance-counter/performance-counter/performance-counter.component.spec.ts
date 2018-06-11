import { Component, Input } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { PerformanceCounterService } from '../../../shared/api/performance-counter.service';
import { configureTestBed } from '../../../shared/unit-test-helper';
import { PerformanceCounterComponent } from './performance-counter.component';

@Component({ selector: 'cd-table-performance-counter', template: '' })
class TablePerformanceCounterStubComponent {
  @Input() serviceType: string;
  @Input() serviceId: string;
}

describe('PerformanceCounterComponent', () => {
  let component: PerformanceCounterComponent;
  let fixture: ComponentFixture<PerformanceCounterComponent>;

  const fakeService = {
    get: (service_type: string, service_id: string) => {
      return new Promise(function(resolve, reject) {
        return [];
      });
    },
    list: () => {
      return new Promise(function(resolve, reject) {
        return {};
      });
    }
  };

  configureTestBed({
    declarations: [PerformanceCounterComponent, TablePerformanceCounterStubComponent],
    imports: [RouterTestingModule],
    providers: [{ provide: PerformanceCounterService, useValue: fakeService }]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(PerformanceCounterComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});

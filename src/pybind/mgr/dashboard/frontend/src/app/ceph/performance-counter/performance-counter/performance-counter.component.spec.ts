import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { BsDropdownModule } from 'ngx-bootstrap';

import {
  TablePerformanceCounterService
} from '../../../shared/api/table-performance-counter.service';
import { PerformanceCounterModule } from '../performance-counter.module';
import { PerformanceCounterComponent } from './performance-counter.component';

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

  beforeEach(
    async(() => {
      TestBed.configureTestingModule({
        imports: [PerformanceCounterModule, BsDropdownModule.forRoot(), RouterTestingModule],
        providers: [{ provide: TablePerformanceCounterService, useValue: fakeService }]
      }).compileComponents();
    })
  );

  beforeEach(() => {
    fixture = TestBed.createComponent(PerformanceCounterComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});

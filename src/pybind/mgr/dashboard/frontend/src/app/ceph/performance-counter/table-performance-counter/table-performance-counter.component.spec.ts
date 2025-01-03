import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { AppModule } from '~/app/app.module';
import { CdTableFetchDataContext } from '~/app/shared/models/cd-table-fetch-data-context';
import { configureTestBed } from '~/testing/unit-test-helper';
import { TablePerformanceCounterComponent } from './table-performance-counter.component';

describe('TablePerformanceCounterComponent', () => {
  let component: TablePerformanceCounterComponent;
  let fixture: ComponentFixture<TablePerformanceCounterComponent>;
  let httpTesting: HttpTestingController;

  configureTestBed({
    imports: [AppModule, HttpClientTestingModule]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(TablePerformanceCounterComponent);
    component = fixture.componentInstance;
    httpTesting = TestBed.inject(HttpTestingController);
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
    expect(component.counters).toEqual([]);
  });

  it('should have columns that are sortable', () => {
    expect(component.columns.every((column) => Boolean(column.prop))).toBeTruthy();
  });

  describe('Error handling', () => {
    const context = new CdTableFetchDataContext(() => undefined);

    beforeEach(() => {
      spyOn(context, 'error');
      component.serviceType = 'osd';
      component.serviceId = '3';
      component.getCounters(context);
    });

    it('should display 404 warning', () => {
      httpTesting
        .expectOne('api/perf_counters/osd/3')
        .error(new ErrorEvent('osd.3 not found'), { status: 404 });
      httpTesting.verify();
      expect(component.counters).toBeNull();
      expect(context.error).not.toHaveBeenCalled();
    });

    it('should call error function of context', () => {
      httpTesting
        .expectOne('api/perf_counters/osd/3')
        .error(new ErrorEvent('Unknown error'), { status: 500 });
      httpTesting.verify();
      expect(component.counters).toEqual([]);
      expect(context.error).toHaveBeenCalled();
    });
  });
});

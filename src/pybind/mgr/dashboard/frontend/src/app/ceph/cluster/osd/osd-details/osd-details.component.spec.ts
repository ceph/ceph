import { HttpClientTestingModule } from '@angular/common/http/testing';
import { DebugElement } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { NgbNavModule } from '@ng-bootstrap/ng-bootstrap';
import { of } from 'rxjs';

import { configureTestBed } from '../../../../../testing/unit-test-helper';
import { OsdService } from '../../../../shared/api/osd.service';
import { SharedModule } from '../../../../shared/shared.module';
import { TablePerformanceCounterComponent } from '../../../performance-counter/table-performance-counter/table-performance-counter.component';
import { DeviceListComponent } from '../../../shared/device-list/device-list.component';
import { SmartListComponent } from '../../../shared/smart-list/smart-list.component';
import { OsdPerformanceHistogramComponent } from '../osd-performance-histogram/osd-performance-histogram.component';
import { OsdDetailsComponent } from './osd-details.component';

describe('OsdDetailsComponent', () => {
  let component: OsdDetailsComponent;
  let fixture: ComponentFixture<OsdDetailsComponent>;
  let debugElement: DebugElement;
  let osdService: OsdService;
  let getDetailsSpy: jasmine.Spy;

  configureTestBed({
    imports: [HttpClientTestingModule, NgbNavModule, SharedModule],
    declarations: [
      OsdDetailsComponent,
      DeviceListComponent,
      SmartListComponent,
      TablePerformanceCounterComponent,
      OsdPerformanceHistogramComponent
    ]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(OsdDetailsComponent);
    component = fixture.componentInstance;

    component.selection = undefined;
    debugElement = fixture.debugElement;
    osdService = debugElement.injector.get(OsdService);

    getDetailsSpy = spyOn(osdService, 'getDetails');

    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should fail creating a histogram', () => {
    const detailDataWithoutHistogram = {
      osd_map: {},
      osd_metadata: {},
      histogram: 'osd down'
    };
    getDetailsSpy.and.returnValue(of(detailDataWithoutHistogram));
    component.osd = { tree: { id: 0 } };
    component.refresh();
    expect(getDetailsSpy).toHaveBeenCalled();
    expect(component.osd.histogram_failed).toBe('osd down');
  });

  it('should succeed creating a histogram', () => {
    const detailDataWithHistogram = {
      osd_map: {},
      osd_metadata: {},
      histogram: {}
    };
    getDetailsSpy.and.returnValue(of(detailDataWithHistogram));
    component.osd = { tree: { id: 0 } };
    component.refresh();
    expect(getDetailsSpy).toHaveBeenCalled();
    expect(component.osd.histogram_failed).toBe('');
  });
});

import { HttpClientTestingModule } from '@angular/common/http/testing';
import { DebugElement } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { of } from 'rxjs';

import { TabsModule } from 'ngx-bootstrap/tabs';

import { configureTestBed, i18nProviders } from '../../../../../testing/unit-test-helper';
import { OsdService } from '../../../../shared/api/osd.service';
import { CdTableSelection } from '../../../../shared/models/cd-table-selection';
import { SharedModule } from '../../../../shared/shared.module';
import { PerformanceCounterModule } from '../../../performance-counter/performance-counter.module';
import { OsdPerformanceHistogramComponent } from '../osd-performance-histogram/osd-performance-histogram.component';
import { OsdDetailsComponent } from './osd-details.component';

describe('OsdDetailsComponent', () => {
  let component: OsdDetailsComponent;
  let fixture: ComponentFixture<OsdDetailsComponent>;
  let debugElement: DebugElement;
  let osdService: OsdService;
  let getDetailsSpy;

  configureTestBed({
    imports: [
      HttpClientTestingModule,
      TabsModule.forRoot(),
      PerformanceCounterModule,
      SharedModule
    ],
    declarations: [OsdDetailsComponent, OsdPerformanceHistogramComponent],
    providers: i18nProviders
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(OsdDetailsComponent);
    component = fixture.componentInstance;

    component.selection = new CdTableSelection();
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
      osd_metdata: {},
      histogram: {}
    };
    getDetailsSpy.and.returnValue(of(detailDataWithHistogram));
    component.osd = { tree: { id: 0 } };
    component.refresh();
    expect(getDetailsSpy).toHaveBeenCalled();
    expect(component.osd.histogram_failed).toBe('');
  });
});

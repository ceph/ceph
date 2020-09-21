import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { NgbNavModule } from '@ng-bootstrap/ng-bootstrap';

import { configureTestBed } from '../../../../../testing/unit-test-helper';
import { SharedModule } from '../../../../shared/shared.module';
import { TablePerformanceCounterComponent } from '../../../performance-counter/table-performance-counter/table-performance-counter.component';
import { DeviceListComponent } from '../../../shared/device-list/device-list.component';
import { SmartListComponent } from '../../../shared/smart-list/smart-list.component';
import { OsdDetailsComponent } from './osd-details.component';

describe('OsdDetailsComponent', () => {
  let component: OsdDetailsComponent;
  let fixture: ComponentFixture<OsdDetailsComponent>;

  configureTestBed({
    imports: [HttpClientTestingModule, NgbNavModule, SharedModule],
    declarations: [
      OsdDetailsComponent,
      DeviceListComponent,
      SmartListComponent,
      TablePerformanceCounterComponent
    ]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(OsdDetailsComponent);
    component = fixture.componentInstance;
    component.selection = undefined;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});

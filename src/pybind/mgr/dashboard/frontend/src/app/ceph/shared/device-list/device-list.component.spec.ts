import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { DeviceListComponent } from './device-list.component';

describe('DeviceListComponent', () => {
  let component: DeviceListComponent;
  let fixture: ComponentFixture<DeviceListComponent>;

  configureTestBed({
    declarations: [DeviceListComponent],
    imports: [SharedModule, HttpClientTestingModule]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(DeviceListComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});

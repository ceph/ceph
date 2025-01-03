import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { NgbNavModule } from '@ng-bootstrap/ng-bootstrap';

import { PerformanceCounterModule } from '~/app/ceph/performance-counter/performance-counter.module';
import { RgwDaemon } from '~/app/ceph/rgw/models/rgw-daemon';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { RgwDaemonDetailsComponent } from './rgw-daemon-details.component';

describe('RgwDaemonDetailsComponent', () => {
  let component: RgwDaemonDetailsComponent;
  let fixture: ComponentFixture<RgwDaemonDetailsComponent>;

  configureTestBed({
    declarations: [RgwDaemonDetailsComponent],
    imports: [SharedModule, PerformanceCounterModule, HttpClientTestingModule, NgbNavModule]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RgwDaemonDetailsComponent);
    component = fixture.componentInstance;
    component.selection = undefined;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should set service id and service map id on changes', () => {
    const daemon = new RgwDaemon();
    daemon.id = 'daemon1';
    daemon.service_map_id = '4832';
    component.selection = daemon;
    component.ngOnChanges();

    expect(component.serviceId).toBe(daemon.id);
    expect(component.serviceMapId).toBe(daemon.service_map_id);
  });
});

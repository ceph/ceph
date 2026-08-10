import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ActivatedRoute } from '@angular/router';

import { NgbNavModule } from '@ng-bootstrap/ng-bootstrap';
import { of } from 'rxjs';

import { RgwDaemon } from '~/app/ceph/rgw/models/rgw-daemon';
import { RgwDaemonService } from '~/app/shared/api/rgw-daemon.service';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { RgwDaemonResourcePageComponent } from './rgw-daemon-resource-page.component';

describe('RgwDaemonResourcePageComponent', () => {
  let component: RgwDaemonResourcePageComponent;
  let fixture: ComponentFixture<RgwDaemonResourcePageComponent>;
  let getSpy: jasmine.Spy;

  const daemon: RgwDaemon = {
    id: 'daemon1',
    service_map_id: '4832',
    version: '18.2.0',
    server_hostname: 'ceph-01',
    realm_name: 'realm1',
    zonegroup_name: 'zg1',
    zonegroup_id: 'zg1-id',
    zone_name: 'zone1',
    default: true,
    port: 80
  };

  configureTestBed({
    declarations: [RgwDaemonResourcePageComponent],
    imports: [SharedModule, HttpClientTestingModule, NgbNavModule],
    providers: [
      {
        provide: ActivatedRoute,
        useValue: {
          snapshot: { data: { section: 'overview' } },
          parent: { data: of({ daemon }) }
        }
      }
    ]
  });

  beforeEach(() => {
    getSpy = spyOn(TestBed.inject(RgwDaemonService), 'get').and.returnValue(
      of({ rgw_metadata: { hostname: 'ceph-01', frontend_config: 'beast' } })
    );
    fixture = TestBed.createComponent(RgwDaemonResourcePageComponent);
    component = fixture.componentInstance;
  });

  it('should create', () => {
    fixture.detectChanges();

    expect(component).toBeTruthy();
    expect(getSpy).toHaveBeenCalledWith(daemon.id);
    expect(component.daemonDetailsFields.length).toBeGreaterThan(0);
  });

  it('should show overview data for the daemon', () => {
    fixture.detectChanges();

    expect(component.selection).toEqual(daemon);
    expect(component.daemonDetailsFields[0].label).toBe('Daemon ID');
    expect(component.daemonDetailsFields.some((field) => field.label === 'Hostname')).toBe(true);
    expect(component.softwareVersionFields.some((field) => field.label === 'Ceph Release')).toBe(
      true
    );
  });
});

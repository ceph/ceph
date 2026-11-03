import { CUSTOM_ELEMENTS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ActivatedRoute, convertToParamMap, ParamMap, Data } from '@angular/router';
import { BehaviorSubject } from 'rxjs';

import { RgwDaemon } from '~/app/ceph/rgw/models/rgw-daemon';
import { RgwDaemonResourceSidebarComponent } from './rgw-daemon-resource-sidebar.component';

describe('RgwDaemonResourceSidebarComponent', () => {
  let component: RgwDaemonResourceSidebarComponent;
  let fixture: ComponentFixture<RgwDaemonResourceSidebarComponent>;
  let paramMapSubject: BehaviorSubject<ParamMap>;
  let dataSubject: BehaviorSubject<Data>;

  beforeEach(async () => {
    // Initialize subjects with default test data
    paramMapSubject = new BehaviorSubject(convertToParamMap({ daemonId: 'daemon-123' }));
    dataSubject = new BehaviorSubject({
      daemon: { id: 'daemon-id-456', server_hostname: 'host.example.com' } as RgwDaemon
    });

    await TestBed.configureTestingModule({
      declarations: [RgwDaemonResourceSidebarComponent],
      providers: [
        {
          provide: ActivatedRoute,
          useValue: {
            paramMap: paramMapSubject.asObservable(),
            data: dataSubject.asObservable()
          }
        }
      ],
      // Ignore unknown custom elements like <cd-sidebar-layout>
      schemas: [CUSTOM_ELEMENTS_SCHEMA]
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RgwDaemonResourceSidebarComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should generate sidebar items with the correct daemonId route', () => {
    expect(component.sidebarItems.length).toBe(2);

    expect(component.sidebarItems[0].label).toBe('Overview');
    expect(component.sidebarItems[0].route).toEqual(['/rgw/daemon', 'daemon-123', 'overview']);

    expect(component.sidebarItems[1].label).toBe('Performance');
    expect(component.sidebarItems[1].route).toEqual(['/rgw/daemon', 'daemon-123', 'performance']);
  });

  it('should set daemonName from daemon.server_hostname if available', () => {
    expect(component.daemonName).toBe('host.example.com');
  });

  it('should fallback to daemon.id for daemonName if server_hostname is undefined', () => {
    dataSubject.next({ daemon: { id: 'daemon-id-456' } as RgwDaemon });
    fixture.detectChanges();

    expect(component.daemonName).toBe('daemon-id-456');
  });

  it('should fallback to route param for daemonName if daemon object is completely absent', () => {
    dataSubject.next({});
    fixture.detectChanges();

    expect(component.daemonName).toBe('daemon-123');
  });

  it('should unsubscribe from observables on destroy', () => {
    const unsubscribeSpy = spyOn(component['sub'], 'unsubscribe');

    component.ngOnDestroy();

    expect(unsubscribeSpy).toHaveBeenCalled();
  });
});

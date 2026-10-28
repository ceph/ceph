import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ActivatedRoute, convertToParamMap } from '@angular/router';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { RouterTestingModule } from '@angular/router/testing';
import { of } from 'rxjs';

import { CephModule } from '~/app/ceph/ceph.module';
import { CephSharedModule } from '~/app/ceph/shared/ceph-shared.module';
import { CoreModule } from '~/app/core/core.module';
import { Permissions } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { HostService } from '~/app/shared/api/host.service';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { HostSidebarComponent } from './host-resource-sidebar.component';

describe('HostSidebarComponent', () => {
  let component: HostSidebarComponent;
  let fixture: ComponentFixture<HostSidebarComponent>;

  configureTestBed({
    imports: [
      BrowserAnimationsModule,
      HttpClientTestingModule,
      RouterTestingModule,
      CephModule,
      CoreModule,
      CephSharedModule,
      SharedModule
    ],
    providers: [
      {
        provide: ActivatedRoute,
        useValue: {
          paramMap: of(convertToParamMap({ hostname: 'localhost' }))
        }
      },
      {
        provide: AuthStorageService,
        useValue: {
          getPermissions: () => new Permissions({ hosts: ['read'], grafana: ['read'] })
        }
      },
      {
        provide: HostService,
        useValue: {
          getDisable: () => false,
          getAllHosts: () =>
            of([
              {
                hostname: 'localhost',
                labels: ['_admin'],
                status: 'available'
              }
            ])
        }
      }
    ]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(HostSidebarComponent);
    component = fixture.componentInstance;
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('Host resource layout', () => {
    beforeEach(() => {
      fixture.detectChanges();
    });

    it('should render the sidebar layout', () => {
      const layout = fixture.nativeElement.querySelector('cd-sidebar-layout');
      expect(layout).toBeTruthy();
    });

    it('should build the sidebar items', () => {
      expect(component.sidebarItems.map((item) => item.label)).toEqual([
        'Overview',
        'Storage Devices',
        'Daemons',
        'Performance'
      ]);
    });

    it('should set the hostname title', () => {
      expect(component.hostname).toBe('localhost');
    });
  });
});

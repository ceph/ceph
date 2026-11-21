import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { of } from 'rxjs';

import { CephModule } from '~/app/ceph/ceph.module';
import { CoreModule } from '~/app/core/core.module';
import { HostService } from '~/app/shared/api/host.service';
import { OsdService } from '~/app/shared/api/osd.service';
import { Permissions } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { CreateClusterReviewComponent } from './create-cluster-review.component';

describe('CreateClusterReviewComponent', () => {
  let component: CreateClusterReviewComponent;
  let fixture: ComponentFixture<CreateClusterReviewComponent>;
  let hostService: HostService;

  configureTestBed({
    imports: [HttpClientTestingModule, SharedModule, CephModule, CoreModule]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(CreateClusterReviewComponent);
    component = fixture.componentInstance;
    hostService = TestBed.inject(HostService);
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('host resource totals', () => {
    it('should keep totalMemory as raw bytes sum (sum-then-format)', () => {
      spyOn(hostService, 'list').and.returnValue(
        of([
          { cpu_count: 2, memory_total_kb: 1024 },
          { cpu_count: 4, memory_total_kb: 2048 },
          { cpu_count: 1, memory_total_kb: undefined }
        ])
      );

      fixture.detectChanges();

      expect(component.hostsCount).toBe(3);
      expect(component.totalCPUs).toBe(7);
      // 1024 KB + 2048 KB -> bytes; invalid host skipped
      expect(component.totalMemory).toBe((1024 + 2048) * 1024);
    });

    it('should leave totalMemory undefined when no host has memory facts', () => {
      spyOn(hostService, 'list').and.returnValue(of([{ cpu_count: 2 }, { cpu_count: 4 }]));

      fixture.detectChanges();

      expect(component.totalMemory).toBeUndefined();
    });
  });

  describe('getDeploymentOptions permission guard', () => {
    let osdService: OsdService;
    let authStorageService: AuthStorageService;

    beforeEach(() => {
      osdService = TestBed.inject(OsdService);
      authStorageService = TestBed.inject(AuthStorageService);
      spyOn(osdService, 'getDeploymentOptions').and.returnValue(of({} as any));
      spyOn(hostService, 'list').and.returnValue(of([]));
      component.isSimpleDeployment = true;
    });

    it('should not call getDeploymentOptions when osd.read is false', () => {
      spyOn(authStorageService, 'getPermissions').and.returnValue(new Permissions({ osd: [] }));
      fixture.detectChanges();
      expect(osdService.getDeploymentOptions).not.toHaveBeenCalled();
    });

    it('should call getDeploymentOptions when osd.read is true', () => {
      spyOn(authStorageService, 'getPermissions').and.returnValue(
        new Permissions({ osd: ['read'] })
      );
      fixture.detectChanges();
      expect(osdService.getDeploymentOptions).toHaveBeenCalled();
    });
  });
});

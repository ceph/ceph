import { TestBed } from '@angular/core/testing';
import { HttpTestingController, provideHttpClientTesting } from '@angular/common/http/testing';

import { SmbService } from './smb.service';
import { configureTestBed } from '~/testing/unit-test-helper';
import { provideHttpClient } from '@angular/common/http';

describe('SmbService', () => {
  let service: SmbService;
  let httpTesting: HttpTestingController;

  configureTestBed({
    providers: [SmbService, provideHttpClient(), provideHttpClientTesting()]
  });

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(SmbService);
    httpTesting = TestBed.inject(HttpTestingController);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should call list clusters', () => {
    service.listClusters().subscribe();
    const req = httpTesting.expectOne('api/smb/cluster');
    expect(req.request.method).toBe('GET');
  });

  it('should call create cluster', () => {
    const request = {
      cluster_resource: {
        resource_type: 'ceph.smb.cluster',
        cluster_id: 'clusterUserTest',
        auth_mode: 'active-directory',
        intent: 'present',
        domain_settings: {
          realm: 'DOMAIN1.SINK.TEST',
          join_sources: [
            {
              source_type: 'resource',
              ref: 'join1-admin'
            }
          ]
        },
        custom_dns: ['192.168.76.204'],
        placement: {
          count: 1
        }
      }
    };
    service.createCluster(request).subscribe();
    const req = httpTesting.expectOne('api/smb/cluster');
    expect(req.request.method).toBe('POST');
  });

  it('should call remove', () => {
    service.removeCluster('cluster_1').subscribe();
    const req = httpTesting.expectOne('api/smb/cluster/cluster_1');
    expect(req.request.method).toBe('DELETE');
  });

  it('should call list shares for a given cluster', () => {
    service.listShares('tango').subscribe();
    const req = httpTesting.expectOne('api/smb/share?cluster_id=tango');
    expect(req.request.method).toBe('GET');
  });

  it('should call list join auth', () => {
    service.listJoinAuths().subscribe();
    const req = httpTesting.expectOne('api/smb/joinauth');
    expect(req.request.method).toBe('GET');
  });

  it('should call list usersgroups', () => {
    service.listUsersGroups().subscribe();
    const req = httpTesting.expectOne('api/smb/usersgroups');
    expect(req.request.method).toBe('GET');
  });

  it('should call create share', () => {
    const request = {
      share_resource: {
        resource_type: 'ceph.smb.share',
        cluster_id: 'clusterUserTest',
        share_id: 'share1',
        intent: 'present',
        name: 'share1name',
        readonly: false,
        browseable: true,
        cephfs: {
          volume: 'fs1',
          path: '/',
          provider: 'samba-vfs'
        }
      }
    };
    service.createShare(request).subscribe();
    const req = httpTesting.expectOne('api/smb/share');
    expect(req.request.method).toBe('POST');
  });

  it('should call delete for given share of a cluster', () => {
    const cluster_id = 'foo';
    const share_id = 'bar';
    service.deleteShare(cluster_id, share_id).subscribe((response: null) => {
      expect(response).toBeUndefined();
    });
    const req = httpTesting.expectOne(`api/smb/share/${cluster_id}/${share_id}`);
    expect(req.request.method).toBe('DELETE');
  });
});

import { TestBed } from '@angular/core/testing';
import { HttpTestingController, provideHttpClientTesting } from '@angular/common/http/testing';

import { APPJSON, APPYAML, SmbService } from './smb.service';
import { configureTestBed } from '~/testing/unit-test-helper';
import { provideHttpClient } from '@angular/common/http';
import {
  CLUSTER_RESOURCE,
  JOIN_AUTH_RESOURCE,
  USERSGROUPS_RESOURCE
} from '~/app/ceph/smb/smb.model';
import { NotificationService } from '../services/notification.service';
import { ToastrModule } from 'ngx-toastr';
import { NotificationType } from '../enum/notification-type.enum';
import { SharedModule } from '../shared.module';

describe('SmbService', () => {
  let service: SmbService;
  let httpTesting: HttpTestingController;
  let notificationShowSpy: jasmine.Spy;

  configureTestBed({
    providers: [SmbService, provideHttpClient(), provideHttpClientTesting()],
    imports: [ToastrModule.forRoot(), SharedModule]
  });

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(SmbService);
    httpTesting = TestBed.inject(HttpTestingController);
    notificationShowSpy = spyOn(TestBed.inject(NotificationService), 'show');
    spyOn(service, 'setDataUploaded');
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  describe('uploadData function', () => {
    it('should do nothing if no file is provided', () => {
      const event: { values: () => Iterator<{ value: any }> } = {
        values: () => ({
          next: () => ({ value: null })
        })
      };

      service.uploadData(event);
      expect(notificationShowSpy).not.toHaveBeenCalled();
      expect(service.setDataUploaded).not.toHaveBeenCalled();
    });

    it('should show an error notification if file type is invalid', () => {
      const fakeFile = { type: 'invalid/type' };
      const event = {
        values: () => ({
          next: () => ({ value: { file: fakeFile } })
        })
      };
      service.uploadData(event);
      expect(notificationShowSpy).toHaveBeenCalledWith(
        NotificationType.error,
        'Invalid file type: only .json or .yaml accepted'
      );
      expect(service.setDataUploaded).not.toHaveBeenCalled();
    });

    it('should process a valid JSON file correctly', () => {
      const jsonContent = '{"foo": "bar"}';
      const fakeFile = { type: APPJSON, name: 'data.json' };
      const event = {
        values: () => ({
          next: () => ({ value: { file: fakeFile } })
        })
      };

      // Create a fake FileReader
      const fakeReader = {
        onload: null as (e: ProgressEvent<FileReader>) => void,
        readAsText(_: any) {
          this.onload({ target: { result: jsonContent } } as ProgressEvent<FileReader>);
        }
      };
      spyOn(window as any, 'FileReader').and.returnValue(fakeReader);
      service.uploadData(event);
      expect(service.setDataUploaded).toHaveBeenCalledWith({ foo: 'bar' });
    });

    it('should process a valid YAML file correctly', () => {
      const yamlContent = 'foo: bar';
      const fakeFile = { type: APPYAML, name: 'data.yaml' };
      const event = {
        values: () => ({
          next: () => ({ value: { file: fakeFile } })
        })
      };

      // Create a fake FileReader
      const fakeReader = {
        onload: null as (e: ProgressEvent<FileReader>) => void,
        readAsText(_: any) {
          this.onload({ target: { result: yamlContent } } as ProgressEvent<FileReader>);
        }
      };

      spyOn(window as any, 'FileReader').and.returnValue(fakeReader);

      service.uploadData(event);
      expect(service.setDataUploaded).toHaveBeenCalledWith({ foo: 'bar' });
    });
  });

  it('should call list clusters', () => {
    service.listClusters().subscribe();
    const req = httpTesting.expectOne('api/smb/cluster');
    expect(req.request.method).toBe('GET');
  });

  it('should call create cluster', () => {
    const request = {
      cluster_resource: {
        resource_type: CLUSTER_RESOURCE,
        cluster_id: 'clusterUserTest',
        auth_mode: 'active-directory',
        intent: 'present',
        domain_settings: {
          realm: 'DOMAIN1.SINK.TEST',
          join_sources: [
            {
              sourceType: 'resource',
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

  it('should call create join auth', () => {
    const request = {
      resource_type: JOIN_AUTH_RESOURCE,
      auth_id: 'foo',
      auth: {
        username: 'user',
        password: 'pass'
      },
      linked_to_cluster: ''
    };
    service.createJoinAuth(request).subscribe();
    const req = httpTesting.expectOne('api/smb/joinauth');
    expect(req.request.method).toBe('POST');
  });

  it('should call list usersgroups', () => {
    service.listUsersGroups().subscribe();
    const req = httpTesting.expectOne('api/smb/usersgroups');
    expect(req.request.method).toBe('GET');
  });

  it('should call create usersgroups', () => {
    const request = {
      resource_type: USERSGROUPS_RESOURCE,
      users_groups_id: 'foo',
      values: {
        users: [
          {
            name: 'user',
            password: 'pass'
          }
        ],
        groups: [
          {
            name: 'bar'
          }
        ]
      },
      linked_to_cluster: ''
    };
    service.createUsersGroups(request).subscribe();
    const req = httpTesting.expectOne('api/smb/usersgroups');
    expect(req.request.method).toBe('POST');
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

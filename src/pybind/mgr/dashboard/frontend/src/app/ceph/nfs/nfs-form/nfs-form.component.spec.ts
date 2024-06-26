import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbTypeaheadModule } from '@ng-bootstrap/ng-bootstrap';
import { ToastrModule } from 'ngx-toastr';
import { Observable, of } from 'rxjs';

import { NfsFormClientComponent } from '~/app/ceph/nfs/nfs-form-client/nfs-form-client.component';
import { NfsFormComponent } from '~/app/ceph/nfs/nfs-form/nfs-form.component';
import { Directory } from '~/app/shared/api/nfs.service';
import { LoadingPanelComponent } from '~/app/shared/components/loading-panel/loading-panel.component';
import { SharedModule } from '~/app/shared/shared.module';
import { ActivatedRouteStub } from '~/testing/activated-route-stub';
import { configureTestBed, RgwHelper } from '~/testing/unit-test-helper';

describe('NfsFormComponent', () => {
  let component: NfsFormComponent;
  let fixture: ComponentFixture<NfsFormComponent>;
  let httpTesting: HttpTestingController;
  let activatedRoute: ActivatedRouteStub;
  let router: Router;

  configureTestBed(
    {
      declarations: [NfsFormComponent, NfsFormClientComponent],
      imports: [
        HttpClientTestingModule,
        ReactiveFormsModule,
        RouterTestingModule,
        SharedModule,
        ToastrModule.forRoot(),
        NgbTypeaheadModule
      ],
      providers: [
        {
          provide: ActivatedRoute,
          useValue: new ActivatedRouteStub({ cluster_id: 'mynfs', export_id: '1' })
        }
      ]
    },
    [LoadingPanelComponent]
  );

  const matchSquash = (backendSquashValue: string, uiSquashValue: string) => {
    component.ngOnInit();
    httpTesting.expectOne('api/nfs-ganesha/cluster').flush(['mynfs']);
    httpTesting.expectOne('ui-api/nfs-ganesha/cephfs/filesystems').flush([{ id: 1, name: 'a' }]);
    httpTesting.expectOne('api/nfs-ganesha/export/mynfs/1').flush({
      fsal: {
        name: 'RGW'
      },
      export_id: 1,
      transports: ['TCP', 'UDP'],
      protocols: [4],
      clients: [],
      squash: backendSquashValue
    });
    httpTesting.verify();
    expect(component.nfsForm.value).toMatchObject({
      squash: uiSquashValue
    });
  };

  beforeEach(() => {
    fixture = TestBed.createComponent(NfsFormComponent);
    component = fixture.componentInstance;
    httpTesting = TestBed.inject(HttpTestingController);
    activatedRoute = <ActivatedRouteStub>TestBed.inject(ActivatedRoute);
    router = TestBed.inject(Router);

    Object.defineProperty(router, 'url', {
      get: jasmine.createSpy('url').and.returnValue('/cephfs/nfs')
    });
    RgwHelper.selectDaemon();
    fixture.detectChanges();

    httpTesting.expectOne('api/nfs-ganesha/cluster').flush(['mynfs']);
    httpTesting.expectOne('ui-api/nfs-ganesha/cephfs/filesystems').flush([{ id: 1, name: 'a' }]);
    httpTesting.verify();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should create the form', () => {
    expect(component.nfsForm.value).toEqual({
      access_type: 'RW',
      clients: [],
      cluster_id: 'mynfs',
      fsal: { fs_name: 'a', name: 'CEPH' },
      path: '/',
      protocolNfsv4: true,
      protocolNfsv3: true,
      pseudo: '',
      sec_label_xattr: 'security.selinux',
      security_label: false,
      squash: 'no_root_squash',
      transportTCP: true,
      transportUDP: true
    });
    expect(component.nfsForm.get('cluster_id').disabled).toBeFalsy();
  });

  it('should prepare data when selecting an cluster', () => {
    component.nfsForm.patchValue({ cluster_id: 'cluster1' });

    component.nfsForm.patchValue({ cluster_id: 'cluster2' });
  });

  it('should not allow changing cluster in edit mode', () => {
    component.isEdit = true;
    component.ngOnInit();
    expect(component.nfsForm.get('cluster_id').disabled).toBeTruthy();
  });

  it('should mark NFSv4 & NFSv3 protocols as enabled always', () => {
    expect(component.nfsForm.get('protocolNfsv4')).toBeTruthy();
    expect(component.nfsForm.get('protocolNfsv3')).toBeTruthy();
  });

  it('should match backend squash values with ui values', () => {
    component.isEdit = true;
    matchSquash('none', 'no_root_squash');
    matchSquash('all', 'all_squash');
    matchSquash('rootid', 'root_id_squash');
    matchSquash('root', 'root_squash');
  });

  describe('should submit request', () => {
    beforeEach(() => {
      component.nfsForm.patchValue({
        access_type: 'RW',
        clients: [],
        cluster_id: 'cluster1',
        fsal: { name: 'CEPH', fs_name: 1 },
        path: '/foo',
        protocolNfsv4: true,
        protocolNfsv3: true,
        pseudo: '/baz',
        squash: 'no_root_squash',
        transportTCP: true,
        transportUDP: true
      });
    });

    it('should call update', () => {
      activatedRoute.setParams({ cluster_id: 'cluster1', export_id: '1' });
      component.isEdit = true;
      component.cluster_id = 'cluster1';
      component.export_id = '1';
      component.nfsForm.patchValue({ export_id: 1 });
      component.submitAction();

      const req = httpTesting.expectOne('api/nfs-ganesha/export/cluster1/1');
      expect(req.request.method).toBe('PUT');
      expect(req.request.body).toEqual({
        access_type: 'RW',
        clients: [],
        cluster_id: 'cluster1',
        export_id: 1,
        fsal: { fs_name: 1, name: 'CEPH', sec_label_xattr: null },
        path: '/foo',
        protocols: [3, 4],
        pseudo: '/baz',
        security_label: false,
        squash: 'no_root_squash',
        transports: ['TCP', 'UDP']
      });
    });

    it('should call update with selected nfs protocol', () => {
      activatedRoute.setParams({ cluster_id: 'cluster1', export_id: '1' });
      component.isEdit = true;
      component.cluster_id = 'cluster1';
      component.export_id = '1';
      component.nfsForm.patchValue({ export_id: 1, protocolNfsv3: false });
      component.submitAction();

      const req = httpTesting.expectOne('api/nfs-ganesha/export/cluster1/1');
      expect(req.request.method).toBe('PUT');
      expect(req.request.body).toEqual({
        access_type: 'RW',
        clients: [],
        cluster_id: 'cluster1',
        export_id: 1,
        fsal: { fs_name: 1, name: 'CEPH', sec_label_xattr: null },
        path: '/foo',
        protocols: [4],
        pseudo: '/baz',
        security_label: false,
        squash: 'no_root_squash',
        transports: ['TCP', 'UDP']
      });
    });

    it('should call create', () => {
      activatedRoute.setParams({ cluster_id: undefined, export_id: undefined });
      component.submitAction();

      const req = httpTesting.expectOne('api/nfs-ganesha/export');
      expect(req.request.method).toBe('POST');
      expect(req.request.body).toEqual({
        access_type: 'RW',
        clients: [],
        cluster_id: 'cluster1',
        fsal: {
          fs_name: 1,
          name: 'CEPH',
          sec_label_xattr: null
        },
        path: '/foo',
        protocols: [3, 4],
        pseudo: '/baz',
        security_label: false,
        squash: 'no_root_squash',
        transports: ['TCP', 'UDP']
      });
    });
  });

  describe('pathExistence', () => {
    beforeEach(() => {
      component['nfsService']['lsDir'] = jest.fn(
        (): Observable<Directory> => of({ paths: ['/path1'] })
      );
      component.nfsForm.get('name').setValue('CEPH');
      component.setPathValidation();
    });

    const testValidator = (pathName: string, valid: boolean, expectedError?: string) => {
      const path = component.nfsForm.get('path');
      path.setValue(pathName);
      path.markAsDirty();
      path.updateValueAndValidity();

      if (valid) {
        expect(path.errors).toBe(null);
      } else {
        expect(path.hasError(expectedError)).toBeTruthy();
      }
    };

    it('path cannot be empty', () => {
      testValidator('', false, 'required');
    });

    it('path that does not exist should be invalid', () => {
      testValidator('/path2', false, 'pathNameNotAllowed');
      expect(component['nfsService']['lsDir']).toHaveBeenCalledTimes(1);
    });

    it('path that exists should be valid', () => {
      testValidator('/path1', true);
      expect(component['nfsService']['lsDir']).toHaveBeenCalledTimes(1);
    });
  });
});

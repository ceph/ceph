import { ComponentFixture, fakeAsync, TestBed, tick } from '@angular/core/testing';
import { SmbShareFormComponent } from './smb-share-form.component';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { SharedModule } from '~/app/shared/shared.module';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { Router } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';
import { ReactiveFormsModule, Validators } from '@angular/forms';
import { of } from 'rxjs';

import {
  CheckboxModule,
  ComboBoxModule,
  GridModule,
  InputModule,
  NumberModule,
  SelectModule
} from 'carbon-components-angular';
import { SmbService } from '~/app/shared/api/smb.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import {
  SMB_BASE_CEPHFS,
  SMB_BASE_RGW,
  SMB_CEPHFS_CONTEXT,
  SMB_CONTEXT,
  SMB_RGW_CONTEXT,
  SmbContext
} from '~/app/shared/constants/smb.constant';

describe('SmbShareFormComponent', () => {
  let component: SmbShareFormComponent;
  let fixture: ComponentFixture<SmbShareFormComponent>;
  let router: Router;

  const setup = async (ctx: SmbContext = SMB_CEPHFS_CONTEXT) => {
    await TestBed.configureTestingModule({
      imports: [
        BrowserAnimationsModule,
        SharedModule,
        HttpClientTestingModule,
        RouterTestingModule,
        ReactiveFormsModule,
        GridModule,
        InputModule,
        NumberModule,
        SelectModule,
        ComboBoxModule,
        CheckboxModule
      ],
      declarations: [SmbShareFormComponent],
      providers: [SmbService, TaskWrapperService, { provide: SMB_CONTEXT, useValue: ctx }]
    }).compileComponents();

    fixture = TestBed.createComponent(SmbShareFormComponent);
    component = fixture.componentInstance;
    router = TestBed.inject(Router);
    component.ngOnInit();
    fixture.detectChanges();
  };

  beforeEach(() => setup());

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should expose the RGW bucket create route via a constant', () => {
    expect(component.routes.RGW_BUCKET_CREATE).toBe('/rgw/bucket/create');
  });

  it('should create the form', () => {
    expect(component.smbShareForm).toBeDefined();
    expect(component.smbShareForm.get('share_id')).toBeTruthy();
    expect(component.smbShareForm.get('volume')).toBeTruthy();
    expect(component.smbShareForm.get('subvolume_group')).toBeTruthy();
    expect(component.smbShareForm.get('prefixedPath')).toBeTruthy();
  });

  it('should update subvolume group when volume changes', () => {
    component.smbShareForm.get('volume').setValue('fs1');
    component.smbShareForm.get('subvolume').setValue('subvol1');
    component.volumeChangeHandler();
    expect(component.smbShareForm.get('subvolume_group').value).toBe('');
    expect(component.smbShareForm.get('subvolume').value).toBe('');
  });

  it('should set the correct subvolume validation', () => {
    component.smbShareForm.get('subvolume_group').setValue('');
    expect(component.smbShareForm.get('subvolume').hasValidator(Validators.required)).toBe(false);
    component.smbShareForm.get('subvolume_group').setValue('otherGroup');
    expect(component.smbShareForm.get('subvolume').hasValidator(Validators.required)).toBe(false);
  });

  it('should call submitAction', () => {
    component.smbShareForm.setValue({
      share_id: 'share1',
      volume: 'fs1',
      name: 'My Share',
      subvolume_group: 'group1',
      subvolume: 'subvol1',
      prefixedPath: '/volumes/fs1/group1/subvol1',
      inputPath: '/',
      bucket: '',
      user_id: '',
      browseable: true,
      readonly: false,
      read_iops_limit: 0,
      write_iops_limit: 0,
      read_bw_limit: 0,
      read_bw_limit_unit: 'MiB',
      write_bw_limit: 0,
      write_bw_limit_unit: 'MiB',
      read_burst_mult: 15,
      write_burst_mult: 15
    });
    component.submitAction();
    expect(component).toBeTruthy();
  });

  it('should navigate to the cluster overview after create or edit', () => {
    const navigateSpy = jest.spyOn(router, 'navigate').mockResolvedValue(true as any);
    jest
      .spyOn(component['taskWrapperService'], 'wrapTaskAroundCall')
      .mockReturnValue(of(null) as any);
    component.clusterId = 'cluster1';
    component.smbShareForm.patchValue({
      share_id: 'share1',
      name: 'My Share',
      volume: 'fs1',
      inputPath: '/'
    });

    component.handleTaskRequest('create');
    expect(navigateSpy).toHaveBeenCalledWith([
      `${SMB_BASE_CEPHFS}/cluster`,
      'cluster1',
      'overview'
    ]);

    navigateSpy.mockClear();
    component.handleTaskRequest('edit');
    expect(navigateSpy).toHaveBeenCalledWith([
      `${SMB_BASE_CEPHFS}/cluster`,
      'cluster1',
      'overview'
    ]);
  });

  it('should navigate to the RGW cluster overview after create or edit', async () => {
    TestBed.resetTestingModule();
    await setup(SMB_RGW_CONTEXT);
    const navigateSpy = jest.spyOn(router, 'navigate').mockResolvedValue(true as any);
    jest
      .spyOn(component['taskWrapperService'], 'wrapTaskAroundCall')
      .mockReturnValue(of(null) as any);
    component.clusterId = 'cluster1';
    component.smbShareForm.patchValue({
      share_id: 'share1',
      name: 'My Share',
      bucket: 'bucket1',
      user_id: 'user1'
    });

    component.handleTaskRequest('create');
    expect(navigateSpy).toHaveBeenCalledWith([`${SMB_BASE_RGW}/cluster`, 'cluster1', 'overview']);

    navigateSpy.mockClear();
    component.handleTaskRequest('edit');
    expect(navigateSpy).toHaveBeenCalledWith([`${SMB_BASE_RGW}/cluster`, 'cluster1', 'overview']);
  });

  describe('QoS', () => {
    it('should have QoS form controls with default values', () => {
      expect(component.smbShareForm.get('read_iops_limit')).toBeTruthy();
      expect(component.smbShareForm.get('write_iops_limit')).toBeTruthy();
      expect(component.smbShareForm.get('read_bw_limit')).toBeTruthy();
      expect(component.smbShareForm.get('read_bw_limit_unit')).toBeTruthy();
      expect(component.smbShareForm.get('write_bw_limit')).toBeTruthy();
      expect(component.smbShareForm.get('write_bw_limit_unit')).toBeTruthy();
      expect(component.smbShareForm.get('read_burst_mult')).toBeTruthy();
      expect(component.smbShareForm.get('write_burst_mult')).toBeTruthy();
      expect(component.smbShareForm.get('read_iops_limit').value).toBe(0);
      expect(component.smbShareForm.get('write_burst_mult').value).toBe(15);
    });

    it('should include QoS fields in buildRequest when set', () => {
      component.clusterId = 'cluster1';
      component.smbShareForm.patchValue({
        share_id: 'share1',
        name: 'My Share',
        volume: 'fs1',
        inputPath: '/',
        read_iops_limit: 1000,
        write_iops_limit: 500,
        read_bw_limit: 100,
        read_bw_limit_unit: 'MiB',
        write_burst_mult: 60
      });
      const request = component.buildRequest();
      expect(request.share_resource.cephfs.qos.read_iops_limit).toBe(1000);
      expect(request.share_resource.cephfs.qos.write_iops_limit).toBe(500);
      expect(request.share_resource.cephfs.qos.read_bw_limit).toBe(100 * 1024 * 1024);
      expect(request.share_resource.cephfs.qos.write_burst_mult).toBe(60);
    });
  });

  describe('RGW share', () => {
    it('should require bucket and omit cephfs in the request', () => {
      component.isRgw = true;
      component.createForm();
      expect(component.smbShareForm.get('bucket').hasValidator(Validators.required)).toBe(true);
      expect(component.smbShareForm.get('volume').hasValidator(Validators.required)).toBe(false);
      component.clusterId = 'cluster1';
      component.smbShareForm.patchValue({
        share_id: 'share1',
        name: 'Share1',
        bucket: 'my-bucket',
        browseable: true,
        readonly: false
      });
      const request = component.buildRequest();
      expect(request.share_resource.rgw.bucket).toBe('my-bucket');
      expect(request.share_resource.rgw.user_id).toBeUndefined();
      expect(request.share_resource.cephfs).toBeUndefined();
    });

    it('should include optional user_id in the request when set', () => {
      component.isRgw = true;
      component.createForm();
      component.clusterId = 'cluster1';
      component.smbShareForm.patchValue({
        share_id: 'share1',
        name: 'Share1',
        bucket: 'my-bucket',
        user_id: 'dashboard',
        browseable: true,
        readonly: false
      });
      const request = component.buildRequest();
      expect(request.share_resource.rgw.bucket).toBe('my-bucket');
      expect(request.share_resource.rgw.user_id).toBe('dashboard');
    });

    it('should omit blank user_id so SMB can resolve the bucket owner', () => {
      component.isRgw = true;
      component.createForm();
      component.clusterId = 'cluster1';
      component.smbShareForm.patchValue({
        share_id: 'share1',
        name: 'Share1',
        bucket: 'my-bucket',
        user_id: '   ',
        browseable: true,
        readonly: false
      });
      const request = component.buildRequest();
      expect(request.share_resource.rgw.user_id).toBeUndefined();
    });

    it('should reject duplicate share IDs in the same cluster', fakeAsync(() => {
      component.clusterId = 'cluster1';
      jest.spyOn(component['smbService'], 'listShares').mockReturnValue(
        of([
          { share_id: 'test', cluster_id: 'cluster1', resource_type: 'ceph.smb.share' },
          { share_id: 'other', cluster_id: 'cluster1', resource_type: 'ceph.smb.share' }
        ] as any)
      );

      component.createForm();
      const shareIdCtrl = component.smbShareForm.get('share_id');
      shareIdCtrl.setValue('test');
      tick();

      expect(shareIdCtrl.errors).toEqual({ shareIdExists: true });
    }));

    it('should clear required error when a bucket value is set', fakeAsync(() => {
      component.isRgw = true;
      component.createForm();
      jest.spyOn(component['rgwBucketService'], 'exists').mockReturnValue(of(true));
      const bucketCtrl = component.smbShareForm.get('bucket');
      bucketCtrl.markAsDirty();
      bucketCtrl.setValue('my-bucket');
      tick(500);
      expect(bucketCtrl.value).toBe('my-bucket');
      expect(bucketCtrl.hasError('required')).toBe(false);
      expect(bucketCtrl.valid).toBe(true);
    }));

    it('should load bucket items for combo box', fakeAsync(() => {
      jest
        .spyOn(component['rgwBucketService'], 'list')
        .mockReturnValue(of(['logs', 'photos', 'backup-logs']) as any);
      component['loadBucketItems']();
      tick();
      expect(component.bucketItems).toEqual([
        { content: 'logs', name: 'logs', selected: false },
        { content: 'photos', name: 'photos', selected: false },
        { content: 'backup-logs', name: 'backup-logs', selected: false }
      ]);
      expect(component.hasNoBuckets).toBe(false);
    }));

    it('should set hasNoBuckets when the bucket list is empty', fakeAsync(() => {
      jest.spyOn(component['rgwBucketService'], 'list').mockReturnValue(of([]) as any);
      component['loadBucketItems']();
      tick();
      expect(component.bucketItems).toEqual([]);
      expect(component.hasNoBuckets).toBe(true);
    }));

    it('should add typed bucket on submit', () => {
      component.isRgw = true;
      component.createForm();
      component.bucketItems = [{ content: 'existing', name: 'existing', selected: false }];
      component.onBucketSubmit({ value: { content: 'new-bucket' } });
      expect(component.bucketItems.map((item) => item.content)).toContain('new-bucket');
      expect(component.smbShareForm.get('bucket').value).toBe('new-bucket');
      expect(component.smbShareForm.get('bucket').dirty).toBe(true);
    });

    describe('suggestBucketOwner', () => {
      beforeEach(() => {
        component.isRgw = true;
        component.createForm();
        component['loadedBucketNames'] = new Set(['my-bucket']);
        component.allRgwUsers = ['admin', 'dashboard'];
      });

      it('should prefill user_id with bucket owner when field is pristine', fakeAsync(() => {
        jest
          .spyOn(component['rgwBucketService'], 'get')
          .mockReturnValue(of({ owner: 'dashboard' }));
        component['suggestBucketOwner']('my-bucket');
        tick();
        expect(component.smbShareForm.get('user_id').value).toBe('dashboard');
      }));

      it('should not prefill when user_id is already set', fakeAsync(() => {
        component.smbShareForm.get('user_id').setValue('admin');
        jest
          .spyOn(component['rgwBucketService'], 'get')
          .mockReturnValue(of({ owner: 'dashboard' }));
        component['suggestBucketOwner']('my-bucket');
        tick();
        expect(component.smbShareForm.get('user_id').value).toBe('admin');
      }));

      it('should not prefill when user_id is dirty', fakeAsync(() => {
        component.smbShareForm.get('user_id').markAsDirty();
        jest
          .spyOn(component['rgwBucketService'], 'get')
          .mockReturnValue(of({ owner: 'dashboard' }));
        component['suggestBucketOwner']('my-bucket');
        tick();
        expect(component.smbShareForm.get('user_id').value).toBeFalsy();
      }));

      it('should skip buckets not in the loaded list', fakeAsync(() => {
        const getSpy = jest.spyOn(component['rgwBucketService'], 'get');
        component['suggestBucketOwner']('unknown-bucket');
        tick();
        expect(getSpy).not.toHaveBeenCalled();
      }));

      it('should not prefill when owner is not in the RGW user list', fakeAsync(() => {
        jest
          .spyOn(component['rgwBucketService'], 'get')
          .mockReturnValue(of({ owner: 'unknown-user' }));
        component['suggestBucketOwner']('my-bucket');
        tick();
        expect(component.smbShareForm.get('user_id').value).toBeFalsy();
      }));
    });
  });
});

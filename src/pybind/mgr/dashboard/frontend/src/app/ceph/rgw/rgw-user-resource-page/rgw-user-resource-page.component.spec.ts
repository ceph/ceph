import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ActivatedRoute } from '@angular/router';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { of } from 'rxjs';

import { RgwUserResourcePageComponent } from './rgw-user-resource-page.component';
import { DimlessBinaryPipe } from '~/app/shared/pipes/dimless-binary.pipe';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { RgwUserS3KeyModalComponent } from '../rgw-user-s3-key-modal/rgw-user-s3-key-modal.component';
import { RgwUserSwiftKeyModalComponent } from '../rgw-user-swift-key-modal/rgw-user-swift-key-modal.component';
import { RgwUser } from '~/app/ceph/rgw/models/rgw-user';

describe('RgwUserResourcePageComponent', () => {
  let component: RgwUserResourcePageComponent;
  let fixture: ComponentFixture<RgwUserResourcePageComponent>;
  let modalCdsServiceMock: any;

  const mockUser: Partial<RgwUser> & { account?: any; managed_user_policies?: any } = {
    uid: 'test-user',
    tenant: 'test-tenant',
    display_name: 'Test User',
    email: 'test@example.com',
    suspended: 0,
    system: false,
    max_buckets: 0,
    caps: [{ type: 'users', perm: '*' }],
    subusers: [{ id: 'sub1', permissions: 'read' }],
    mfa_ids: ['mfa1', 'mfa2'],
    managed_user_policies: ['arn:aws:iam::123:policy/Pol1'],
    user_quota: {
      enabled: true,
      max_size: 1024,
      max_objects: 100,
      max_size_kb: 1,
      check_on_raw: false
    },
    bucket_quota: {
      enabled: false,
      max_size: -1,
      max_objects: -1,
      max_size_kb: -1,
      check_on_raw: false
    },
    stats: {
      size_actual: 512,
      num_objects: 25,
      size: 0,
      size_utilized: 0,
      size_kb: 0,
      size_kb_actual: 0,
      size_kb_utilized: 0
    },
    keys: [{ user: 'test-user', access_key: 'A1', secret_key: 'S1', active: true }],
    swift_keys: [{ user: 'test-user:swift', secret_key: 'S2', active: true }],
    account: { id: 'acc1', name: 'acc-name', tenant: 'acc-tenant' },
    type: 'rgw'
  };

  const activatedRouteMock = {
    snapshot: { data: { section: 'overview' } },
    parent: {
      data: of({ user: mockUser })
    }
  };

  class MockDimlessBinaryPipe {
    transform(value: any): string {
      return `${value} B`;
    }
  }

  beforeEach(async () => {
    // Create a Jest mock object instead of a Jasmine SpyObj
    modalCdsServiceMock = {
      show: jest.fn()
    };

    await TestBed.configureTestingModule({
      declarations: [RgwUserResourcePageComponent],
      providers: [
        { provide: ActivatedRoute, useValue: activatedRouteMock },
        { provide: ModalCdsService, useValue: modalCdsServiceMock },
        { provide: DimlessBinaryPipe, useClass: MockDimlessBinaryPipe }
      ],
      schemas: [NO_ERRORS_SCHEMA]
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RgwUserResourcePageComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should set section from route snapshot on init', () => {
    expect(component.section).toBe('overview');
  });

  it('should set notFound to true if user is null', () => {
    component['applyUser'](null);
    expect(component.notFound).toBe(true);
    expect(component.user).toBeUndefined();
    expect(component.overviewFields).toEqual([]);
    expect(component.keys).toEqual([]);
  });

  it('should populate overviewFields correctly based on user data', () => {
    expect(component.overviewFields).toBeDefined();
    expect(component.overviewFields.length).toBeGreaterThan(0);

    const mfaField = component.overviewFields.find((f) => f.label === 'MFAs (Id)');
    expect(mfaField?.value).toBe('mfa1, mfa2');

    const subuserField = component.overviewFields.find((f) => f.label === 'Subusers');
    expect(subuserField?.value).toBe('sub1 (read)');

    const capsField = component.overviewFields.find((f) => f.label === 'Capabilities');
    expect(capsField?.value).toBe('users (*)');

    const maxBucketsField = component.overviewFields.find((f) => f.label === 'Max buckets');
    expect(maxBucketsField?.value).toBe('Unlimited');
  });

  it('should calculate quota usage text correctly', () => {
    const sizeLimitField = component.overviewFields.find((f) => f.label === 'Capacity limit');
    expect(sizeLimitField?.value).toBe('50.0%');

    const objectLimitField = component.overviewFields.find((f) => f.label === 'Object limit');
    expect(objectLimitField?.value).toBe('25.0%');
  });

  it('should return null for quota usage if disabled', () => {
    const quotaText = component['getQuotaUsageText'].call(
      { user: { user_quota: mockUser.bucket_quota, stats: mockUser.stats } },
      'size'
    );
    expect(quotaText).toBeNull();
  });

  it('should build user and bucket quota display values properly', () => {
    expect(component.userQuota['Enabled']).toBe('Yes');
    expect(component.userQuota['Maximum size']).toBe('1024 B');
    expect(component.userQuota['Maximum objects']).toBe(100);

    expect(component.bucketQuota['Enabled']).toBe('No');
    expect(component.bucketQuota['Maximum size']).toBe('-');
    expect(component.bucketQuota['Maximum objects']).toBe('-');
  });

  it('should process S3 and Swift keys correctly', () => {
    expect(component.keys.length).toBe(2);

    expect(component.keys[0].type).toBe('S3');
    expect(component.keys[0].username).toBe('test-user');

    expect(component.keys[1].type).toBe('Swift');
    expect(component.keys[1].username).toBe('test-user:swift');
  });

  it('should show Key Modal for S3 keys', () => {
    const s3KeyRow: any = { type: 'S3', ref: { user: 'u1', access_key: 'a1', secret_key: 's1' } };

    // Create Jest mock functions for the modal reference
    const modalRefMock = {
      setViewing: jest.fn(),
      setValues: jest.fn()
    };
    modalCdsServiceMock.show.mockReturnValue(modalRefMock);

    component.showKeyModal(s3KeyRow);

    expect(modalCdsServiceMock.show).toHaveBeenCalledWith(RgwUserS3KeyModalComponent);
    expect(modalRefMock.setViewing).toHaveBeenCalled();
    expect(modalRefMock.setValues).toHaveBeenCalledWith('u1', 'a1', 's1');
  });

  it('should show Key Modal for Swift keys', () => {
    const swiftKeyRow: any = { type: 'Swift', ref: { user: 'u2', secret_key: 's2' } };

    // Create Jest mock functions for the modal reference
    const modalRefMock = {
      setViewing: jest.fn(),
      setValues: jest.fn()
    };
    modalCdsServiceMock.show.mockReturnValue(modalRefMock);

    component.showKeyModal(swiftKeyRow);

    expect(modalCdsServiceMock.show).toHaveBeenCalledWith(RgwUserSwiftKeyModalComponent);
    expect(modalRefMock.setViewing).not.toHaveBeenCalled();
    expect(modalRefMock.setValues).toHaveBeenCalledWith('u2', 's2');
  });

  it('should not throw error if showKeyModal is called without a key', () => {
    expect(() => component.showKeyModal(undefined as any)).not.toThrow();
    expect(modalCdsServiceMock.show).not.toHaveBeenCalled();
  });

  it('should unsubscribe on destroy', () => {
    // Use jest.spyOn instead of spyOn
    const subSpy = jest.spyOn(component['sub'], 'unsubscribe');
    component.ngOnDestroy();
    expect(subSpy).toHaveBeenCalled();
  });
});

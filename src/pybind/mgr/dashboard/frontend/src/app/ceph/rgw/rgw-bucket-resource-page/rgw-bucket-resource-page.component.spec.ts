import { ComponentFixture, TestBed } from '@angular/core/testing';
import { NO_ERRORS_SCHEMA, ChangeDetectorRef } from '@angular/core';
import { ActivatedRoute, convertToParamMap } from '@angular/router';
import { BehaviorSubject, of } from 'rxjs';

import { RgwBucketResourcePageComponent } from './rgw-bucket-resource-page.component';
import { Bucket } from '../models/rgw-bucket';
import { RgwBucketService } from '~/app/shared/api/rgw-bucket.service';

describe('RgwBucketResourcePageComponent', () => {
  let component: RgwBucketResourcePageComponent;
  let fixture: ComponentFixture<RgwBucketResourcePageComponent>;
  let paramMapSubject: BehaviorSubject<any>;
  let mockRgwBucketService: any;
  let mockChangeDetectorRef: any;

  beforeEach(async () => {
    paramMapSubject = new BehaviorSubject(
      convertToParamMap({ bid: 'test-bucket', owner: 'Account1' })
    );

    // Setup Jest mocks
    mockRgwBucketService = {
      get: jest.fn().mockReturnValue(
        of({
          bid: 'test-bucket',
          owner: 'Account1',
          bucket_size: 1024,
          replication: { sync_policy_active: true, replication_rules_configured: false, policy: {} }
        })
      ),
      getLockDays: jest.fn().mockReturnValue(5),
      getBucketRateLimit: jest.fn().mockReturnValue(
        of({
          bucket_ratelimit: { max_read_ops: 100 }
        })
      )
    };

    mockChangeDetectorRef = {
      detectChanges: jest.fn()
    };

    await TestBed.configureTestingModule({
      declarations: [RgwBucketResourcePageComponent],
      providers: [
        {
          provide: ActivatedRoute,
          useValue: {
            snapshot: {
              data: { section: 'overview' }
            },
            parent: {
              paramMap: paramMapSubject.asObservable()
            }
          }
        },
        { provide: RgwBucketService, useValue: mockRgwBucketService },
        { provide: ChangeDetectorRef, useValue: mockChangeDetectorRef }
      ],
      schemas: [NO_ERRORS_SCHEMA]
    }).compileComponents();

    fixture = TestBed.createComponent(RgwBucketResourcePageComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should initialize section, bid, and owner from route', () => {
    expect(component.section).toBe('overview');
    expect(component.bid).toBe('test-bucket');
    expect(component.owner).toBe('Account1');
  });

  it('should clear selection and overviewFields when bid is empty', () => {
    paramMapSubject.next(convertToParamMap({ bid: '', owner: 'Account1' }));

    expect(component.selection).toBeUndefined();
    expect(component.overviewFields).toEqual([]);
  });

  it('should fetch bucket details and trigger data extraction', () => {
    jest.spyOn(component, 'extractDetailsFromResponse');

    // re-trigger parameter update to fire updateBucketDetails manually
    paramMapSubject.next(convertToParamMap({ bid: 'test-bucket', owner: 'Account1' }));

    expect(mockRgwBucketService.get).toHaveBeenCalledWith('test-bucket');
    expect(mockRgwBucketService.getLockDays).toHaveBeenCalled();
    expect(component.selection).toBeDefined();
    expect(component.selection.lock_retention_period_days).toBe(5);
    expect(component.extractDetailsFromResponse).toHaveBeenCalled();
  });

  it('should extract replication details correctly (Sync Policy Only)', () => {
    component.selection = {
      replication: {
        sync_policy_active: true,
        replication_rules_configured: false,
        policy: {}
      }
    } as unknown as Bucket;
    (component as any).extractReplicationDetails();

    expect(component.replicationData.sync_policy_active).toBe(true);
    expect(component.hasSyncPolicyOnly).toBe(true);
    expect(component.replicationStatus).toBe('Enabled');
  });

  it('should extract replication details correctly (Rules Configured)', () => {
    component.selection = {
      replication: {
        sync_policy_active: false,
        replication_rules_configured: true,
        policy: { Rule: { Status: 'Active' } }
      }
    } as unknown as Bucket;
    (component as any).extractReplicationDetails();

    expect(component.replicationData.replication_rules_configured).toBe(true);
    expect(component.hasSyncPolicyOnly).toBe(false);
    expect(component.replicationStatus).toBe('Active');
  });

  it('should fetch bucket rate limit', () => {
    component.selection = { bid: 'test-bucket' } as unknown as Bucket;
    (component as any).fetchRateLimit();

    expect(mockRgwBucketService.getBucketRateLimit).toHaveBeenCalledWith('test-bucket');
    expect(component.bucketRateLimit).toEqual({ max_read_ops: 100 } as any);
  });

  it('should parse XML ACL accurately', () => {
    const dummyXml = `
      <AccessControlPolicy>
        <AccessControlList>
          <Grant>
            <Grantee><ID>Account1</ID></Grantee>
            <Permission>FULL_CONTROL</Permission>
          </Grant>
          <Grant>
            <Grantee><URI>http://acs.amazonaws.com/groups/global/AllUsers</URI></Grantee>
            <Permission>READ</Permission>
          </Grant>
        </AccessControlList>
      </AccessControlPolicy>
    `;

    const result = component.parseXmlAcl(dummyXml, 'Account1');

    expect(result['Owner']).toBe('FULL_CONTROL');
    expect(result['AllUsers']).toContain('READ');
  });

  it('should extract lifecycle details and match bucket progress', () => {
    component.lifecycleFormat = 'json';
    component.selection = {
      bucket: 'test-bucket',
      lifecycle: null,
      lifecycle_progress: [
        { bucket: 'other-bucket', status: 'COMPLETE', started: '2025-01-01' },
        { bucket: 'test-bucket', status: 'PROCESSING', started: '2025-01-02' }
      ]
    } as unknown as Bucket;

    component.extractLifecycleDetails();

    expect(component.selection.lifecycle).toEqual({});
    expect(component.lifecycleProgress.status).toBe('PROCESSING');
    expect(component.lifecycleProgress.bucket).toBe('test-bucket');
  });

  it('should update lifecycle format from switcher event', () => {
    jest.spyOn(component, 'updateLifecycleFormatTo');

    component.updateLifecycleFormatFromSwitcher({ name: 'xml' } as any);

    expect(component.updateLifecycleFormatTo).toHaveBeenCalledWith('xml');
  });

  it('should unsubscribe on destroy', () => {
    component.ngOnDestroy();
    expect((component as any).sub.closed).toBe(true);
  });
});

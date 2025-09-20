import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { of } from 'rxjs';

import { RgwBucketService } from '~/app/shared/api/rgw-bucket.service';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { RgwBucketDetailsComponent } from './rgw-bucket-details.component';
import { NgbNavModule } from '@ng-bootstrap/ng-bootstrap';

describe('RgwBucketDetailsComponent', () => {
  let component: RgwBucketDetailsComponent;
  let fixture: ComponentFixture<RgwBucketDetailsComponent>;
  let rgwBucketService: RgwBucketService;
  let rgwBucketServiceGetSpy: jasmine.Spy;

  configureTestBed({
    declarations: [RgwBucketDetailsComponent],
    imports: [SharedModule, HttpClientTestingModule, NgbNavModule]
  });

  beforeEach(() => {
    rgwBucketService = TestBed.inject(RgwBucketService);
    rgwBucketServiceGetSpy = spyOn(rgwBucketService, 'get');
    rgwBucketServiceGetSpy.and.returnValue(of(null));
    fixture = TestBed.createComponent(RgwBucketDetailsComponent);
    component = fixture.componentInstance;
    component.selection = new CdTableSelection();
    component.selection = { bid: 'bucket', bucket_quota: { enabled: false, max_size: 0 } };
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should retrieve bucket full info', () => {
    component.selection = { bid: 'bucket' };
    component.ngOnChanges();
    expect(rgwBucketServiceGetSpy).toHaveBeenCalled();
  });
  it('should retrieve bucket details and set selection when selection is provided', () => {
    const bucket = { bid: 'bucket', acl: '<xml></xml>', owner: 'owner' };
    rgwBucketServiceGetSpy.and.returnValue(of(bucket));
    component.selection = { bid: 'bucket' };
    component.ngOnChanges();
    expect(rgwBucketServiceGetSpy).toHaveBeenCalledWith('bucket');
    expect(component.selection).toEqual(jasmine.objectContaining(bucket));
  });

  it('should set default lifecycle when lifecycleFormat is json and lifecycle is not provided', () => {
    const bucket = { bid: 'bucket', acl: '<xml></xml>', owner: 'owner' };
    rgwBucketServiceGetSpy.and.returnValue(of(bucket));
    component.selection = { bid: 'bucket' };
    component.lifecycleFormat = 'json';
    component.extraxtDetailsfromResponse();
    expect(component.selection.lifecycle).toEqual({});
  });

  it('should parse ACL and set aclPermissions', () => {
    const bucket = { bid: 'bucket', acl: '<xml></xml>', owner: 'owner' };
    rgwBucketServiceGetSpy.and.returnValue(of(bucket));
    spyOn(component, 'parseXmlAcl').and.returnValue({ Owner: ['READ'] });
    component.selection = { bid: 'bucket' };
    component.ngOnChanges();
    expect(component.aclPermissions).toEqual({ Owner: ['READ'] });
  });

  it('should set replicationStatus when replication status is provided', () => {
    const bucket = {
      bid: 'bucket',
      acl: '<xml></xml>',
      owner: 'owner',
      replication: { Rule: { Status: 'Enabled' } }
    };
    rgwBucketServiceGetSpy.and.returnValue(of(bucket));
    component.selection = { bid: 'bucket' };
    component.ngOnChanges();
    expect(component.replicationStatus).toBe('Disabled');
  });

  it('should set bucketRateLimit when getBucketRateLimit is called', () => {
    const rateLimit = { bucket_ratelimit: { max_size: 1000 } };
    spyOn(rgwBucketService, 'getBucketRateLimit').and.returnValue(of(rateLimit));
    component.selection = { bid: 'bucket' };
    component.extraxtDetailsfromResponse();
    expect(component.bucketRateLimit).toEqual(rateLimit.bucket_ratelimit);
  });

  it('should return default permissions when ACL is empty', () => {
    const xml = `
    <AccessControlPolicy>
      <AccessControlList>
        <Grant>
        </Grant>
      </AccessControlList>
    </AccessControlPolicy>
  `;
    const result = component.parseXmlAcl(xml, 'owner');
    expect(result).toEqual({
      Owner: ['-'],
      AllUsers: ['-'],
      AuthenticatedUsers: ['-']
    });
  });

  it('should return owner permissions when ACL contains owner ID', () => {
    const xml = `
    <AccessControlPolicy>
      <AccessControlList>
        <Grant>
          <Grantee>
            <ID>owner</ID>
          </Grantee>
          <Permission>FULL_CONTROL</Permission>
        </Grant>
      </AccessControlList>
    </AccessControlPolicy>
  `;
    const result = component.parseXmlAcl(xml, 'owner');
    expect(result.Owner).toEqual('FULL_CONTROL');
  });

  it('should return group permissions when ACL contains group URI', () => {
    const xml = `
    <AccessControlPolicy>
      <AccessControlList>
        <Grant>
          <Grantee>
            <URI>http://acs.amazonaws.com/groups/global/AllUsers</URI>
          </Grantee>
          <Permission>READ</Permission>
        </Grant>
      </AccessControlList>
    </AccessControlPolicy>
  `;
    const result = component.parseXmlAcl(xml, 'owner');
    expect(result.AllUsers).toEqual(['-']);
  });

  it('should handle multiple grants correctly', () => {
    const xml = `
    <AccessControlPolicy>
      <AccessControlList>
        <Grant>
          <Grantee>
            <URI>http://acs.amazonaws.com/groups/global/AllUsers</URI>
          </Grantee>
          <Permission>READ</Permission>
        </Grant>
        <Grant>
          <Grantee>
            <URI>http://acs.amazonaws.com/groups/global/AuthenticatedUsers</URI>
          </Grantee>
          <Permission>WRITE</Permission>
        </Grant>
      </AccessControlList>
    </AccessControlPolicy>
  `;
    const result = component.parseXmlAcl(xml, 'owner');
    expect(result.AllUsers).toEqual(['READ']);
    expect(result.AuthenticatedUsers).toEqual(['WRITE']);
  });
});

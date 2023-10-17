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
});

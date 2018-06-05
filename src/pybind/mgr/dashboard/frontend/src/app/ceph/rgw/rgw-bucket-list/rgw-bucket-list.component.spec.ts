import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { ModalModule, TabsModule } from 'ngx-bootstrap';

import { RgwBucketService } from '../../../shared/api/rgw-bucket.service';
import { SharedModule } from '../../../shared/shared.module';
import { configureTestBed } from '../../../shared/unit-test-helper';
import { RgwBucketDetailsComponent } from '../rgw-bucket-details/rgw-bucket-details.component';
import { RgwBucketListComponent } from './rgw-bucket-list.component';

describe('RgwBucketListComponent', () => {
  let component: RgwBucketListComponent;
  let fixture: ComponentFixture<RgwBucketListComponent>;

  configureTestBed({
    declarations: [RgwBucketListComponent, RgwBucketDetailsComponent],
    imports: [
      RouterTestingModule,
      ModalModule.forRoot(),
      SharedModule,
      TabsModule.forRoot(),
      HttpClientTestingModule
    ],
    providers: [RgwBucketService]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RgwBucketListComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});

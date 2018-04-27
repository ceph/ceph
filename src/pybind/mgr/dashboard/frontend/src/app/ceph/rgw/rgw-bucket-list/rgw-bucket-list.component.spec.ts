import { HttpClientModule } from '@angular/common/http';
import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { BsDropdownModule } from 'ngx-bootstrap';
import { TabsModule } from 'ngx-bootstrap/tabs';

import { RgwBucketService } from '../../../shared/api/rgw-bucket.service';
import { DataTableModule } from '../../../shared/datatable/datatable.module';
import { SharedModule } from '../../../shared/shared.module';
import { RgwBucketDetailsComponent } from '../rgw-bucket-details/rgw-bucket-details.component';
import { RgwBucketListComponent } from './rgw-bucket-list.component';

describe('RgwBucketListComponent', () => {
  let component: RgwBucketListComponent;
  let fixture: ComponentFixture<RgwBucketListComponent>;

  const fakeRgwBucketService = {
    list: () => {
      return new Promise(function(resolve) {
        resolve([]);
      });
    }
  };

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [
        RgwBucketListComponent,
        RgwBucketDetailsComponent
      ],
      imports: [
        HttpClientModule,
        RouterTestingModule,
        BsDropdownModule.forRoot(),
        TabsModule.forRoot(),
        DataTableModule,
        SharedModule
      ],
      providers: [{ provide: RgwBucketService, useValue: fakeRgwBucketService }]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(RgwBucketListComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});

import { Component, Input } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { ModalModule } from 'ngx-bootstrap';

import { RgwBucketService } from '../../../shared/api/rgw-bucket.service';
import { CdTableColumn } from '../../../shared/models/cd-table-column';
import { CdTableSelection } from '../../../shared/models/cd-table-selection';
import { configureTestBed } from '../../../shared/unit-test-helper';
import { RgwBucketListComponent } from './rgw-bucket-list.component';

@Component({ selector: 'cd-rgw-bucket-details', template: '' })
class RgwBucketDetailsStubComponent {
  @Input() selection: CdTableSelection;
}

@Component({ selector: 'cd-table', template: '' })
class TableStubComponent {
  @Input() data: any[];
  @Input() columns: CdTableColumn[];
  @Input() autoReload: any = 5000;
}

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

  configureTestBed({
    declarations: [RgwBucketListComponent, RgwBucketDetailsStubComponent, TableStubComponent],
    imports: [RouterTestingModule, ModalModule.forRoot()],
    providers: [{ provide: RgwBucketService, useValue: fakeRgwBucketService }]
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

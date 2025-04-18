import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import _ from 'lodash';
import { ToastrModule } from 'ngx-toastr';

import { CephModule } from '~/app/ceph/ceph.module';
import { CoreModule } from '~/app/core/core.module';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { CreateClusterReviewComponent } from './create-cluster-review.component';

describe('CreateClusterReviewComponent', () => {
  let component: CreateClusterReviewComponent;
  let fixture: ComponentFixture<CreateClusterReviewComponent>;

  configureTestBed({
    imports: [HttpClientTestingModule, SharedModule, ToastrModule.forRoot(), CephModule, CoreModule]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(CreateClusterReviewComponent);
    component = fixture.componentInstance;
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});

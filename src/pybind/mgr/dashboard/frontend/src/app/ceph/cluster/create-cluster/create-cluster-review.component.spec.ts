import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import _ from 'lodash';
import { of } from 'rxjs';

import { CephModule } from '~/app/ceph/ceph.module';
import { CoreModule } from '~/app/core/core.module';
import { HostService } from '~/app/shared/api/host.service';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { CreateClusterReviewComponent } from './create-cluster-review.component';

describe('CreateClusterReviewComponent', () => {
  let component: CreateClusterReviewComponent;
  let fixture: ComponentFixture<CreateClusterReviewComponent>;
  let hostService: HostService;
  let hostListSpy: jasmine.Spy;

  configureTestBed({
    imports: [HttpClientTestingModule, SharedModule, CoreModule, CephModule]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(CreateClusterReviewComponent);
    component = fixture.componentInstance;
    hostService = TestBed.inject(HostService);
    hostListSpy = spyOn(hostService, 'list');
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should verify host metadata calculations', () => {
    const hostnames = ['ceph.test1', 'ceph.test2'];
    const payload = [
      {
        hostname: hostnames[0],
        ceph_version: 'ceph version Development',
        labels: ['foo', 'bar']
      },
      {
        hostname: hostnames[1],
        ceph_version: 'ceph version Development',
        labels: ['foo1', 'bar1']
      }
    ];
    hostListSpy.and.callFake(() => of(payload));
    fixture.detectChanges();
    expect(hostListSpy).toHaveBeenCalled();

    expect(component.hostsCount).toBe(2);
    expect(component.uniqueLabels.size).toBe(4);
    const labels = ['foo', 'bar', 'foo1', 'bar1'];

    labels.forEach((label) => {
      expect(component.labelOccurrences[label]).toBe(1);
    });
  });
});

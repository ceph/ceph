import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { By } from '@angular/platform-browser';
import { RouterTestingModule } from '@angular/router/testing';

import { ToastrModule } from 'ngx-toastr';

import { ClusterService } from '~/app/shared/api/cluster.service';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { CreateClusterComponent } from './create-cluster.component';

describe('CreateClusterComponent', () => {
  let component: CreateClusterComponent;
  let fixture: ComponentFixture<CreateClusterComponent>;
  let clusterService: ClusterService;

  configureTestBed({
    declarations: [CreateClusterComponent],
    imports: [HttpClientTestingModule, RouterTestingModule, ToastrModule.forRoot(), SharedModule]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(CreateClusterComponent);
    component = fixture.componentInstance;
    clusterService = TestBed.inject(ClusterService);
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should have the heading "Welcome to Ceph Dashboard"', () => {
    const heading = fixture.debugElement.query(By.css('h3')).nativeElement;
    expect(heading.innerHTML).toBe('Welcome to Ceph Dashboard');
  });

  it('should call updateStatus when cluster creation is skipped', () => {
    const clusterServiceSpy = spyOn(clusterService, 'updateStatus').and.callThrough();
    expect(clusterServiceSpy).not.toHaveBeenCalled();
    component.skipClusterCreation();
    expect(clusterServiceSpy).toHaveBeenCalledTimes(1);
  });
});

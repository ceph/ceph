import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { By } from '@angular/platform-browser';
import { RouterTestingModule } from '@angular/router/testing';

import { ToastrModule } from 'ngx-toastr';

import { CephModule } from '~/app/ceph/ceph.module';
import { CoreModule } from '~/app/core/core.module';
import { ClusterService } from '~/app/shared/api/cluster.service';
import { HostService } from '~/app/shared/api/host.service';
import { WizardStepsService } from '~/app/shared/services/wizard-steps.service';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { CreateClusterComponent } from './create-cluster.component';

describe('CreateClusterComponent', () => {
  let component: CreateClusterComponent;
  let fixture: ComponentFixture<CreateClusterComponent>;
  let clusterService: ClusterService;
  let wizardStepService: WizardStepsService;
  let hostService: HostService;

  configureTestBed({
    imports: [
      HttpClientTestingModule,
      RouterTestingModule,
      ToastrModule.forRoot(),
      SharedModule,
      CoreModule,
      CephModule
    ]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(CreateClusterComponent);
    component = fixture.componentInstance;
    clusterService = TestBed.inject(ClusterService);
    wizardStepService = TestBed.inject(WizardStepsService);
    hostService = TestBed.inject(HostService);
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

  it('should show the wizard when cluster creation is started', () => {
    component.createCluster();
    fixture.detectChanges();
    const nativeEl = fixture.debugElement.nativeElement;
    expect(nativeEl.querySelector('cd-wizard')).not.toBe(null);
  });

  it('should have title Add Hosts', () => {
    component.createCluster();
    fixture.detectChanges();
    const heading = fixture.debugElement.query(By.css('.title')).nativeElement;
    expect(heading.innerHTML).toBe('Add Hosts');
  });

  it('should show the host list when cluster creation as first step', () => {
    component.createCluster();
    fixture.detectChanges();
    const nativeEl = fixture.debugElement.nativeElement;
    expect(nativeEl.querySelector('cd-hosts')).not.toBe(null);
  });

  it('should move to next step and show the second page', () => {
    const wizardStepServiceSpy = spyOn(wizardStepService, 'moveToNextStep').and.callThrough();
    const hostServiceSpy = spyOn(hostService, 'list').and.callThrough();
    component.createCluster();
    fixture.detectChanges();
    component.onNextStep();
    fixture.detectChanges();
    const heading = fixture.debugElement.query(By.css('.title')).nativeElement;
    expect(wizardStepServiceSpy).toHaveBeenCalledTimes(1);
    expect(hostServiceSpy).toBeCalledTimes(1);
    expect(heading.innerHTML).toBe('Review');
  });

  it('should show the button labels correctly', () => {
    component.createCluster();
    fixture.detectChanges();
    let submitBtnLabel = component.showSubmitButtonLabel();
    expect(submitBtnLabel).toEqual('Next');
    let cancelBtnLabel = component.showCancelButtonLabel();
    expect(cancelBtnLabel).toEqual('Cancel');

    // Last page of the wizard
    component.onNextStep();
    fixture.detectChanges();
    submitBtnLabel = component.showSubmitButtonLabel();
    expect(submitBtnLabel).toEqual('Create Cluster');
    cancelBtnLabel = component.showCancelButtonLabel();
    expect(cancelBtnLabel).toEqual('Back');
  });
});

import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { By } from '@angular/platform-browser';
import { RouterTestingModule } from '@angular/router/testing';

import { CephModule } from '~/app/ceph/ceph.module';
import { CoreModule } from '~/app/core/core.module';
import { HostService } from '~/app/shared/api/host.service';
import { OsdService } from '~/app/shared/api/osd.service';
import { ConfirmationModalComponent } from '~/app/shared/components/confirmation-modal/confirmation-modal.component';
import { AppConstants } from '~/app/shared/constants/app.constants';
import { ModalService } from '~/app/shared/services/modal.service';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { CreateClusterComponent } from './create-cluster.component';
import { CreateClusterStep2Component } from './create-cluster-step-2/create-cluster-step-2.component';
import { CreateClusterStep3Component } from './create-cluster-step-3/create-cluster-step-3.component';

describe('CreateClusterComponent', () => {
  let component: CreateClusterComponent;
  let fixture: ComponentFixture<CreateClusterComponent>;
  let hostService: HostService;
  let osdService: OsdService;
  let modalServiceShowSpy: jasmine.Spy;
  const projectConstants: typeof AppConstants = AppConstants;

  configureTestBed({
    imports: [HttpClientTestingModule, RouterTestingModule, SharedModule, CoreModule, CephModule]
  });

  beforeEach(() => {
    TestBed.overrideComponent(CreateClusterStep3Component, {
      set: { template: '<div class="create-cluster-step-3"></div>' }
    });

    fixture = TestBed.createComponent(CreateClusterComponent);
    component = fixture.componentInstance;
    hostService = TestBed.inject(HostService);
    osdService = TestBed.inject(OsdService);
    modalServiceShowSpy = spyOn(TestBed.inject(ModalService), 'show').and.returnValue({
      close: jest.fn()
    });
    fixture.detectChanges();
  });

  const openTearsheet = () => {
    component.createCluster();
    fixture.detectChanges();
  };

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should have project name in welcome screen', () => {
    component.startClusterCreation = true;
    fixture.detectChanges();

    const nativeEl = fixture.debugElement.nativeElement;
    expect(nativeEl.textContent).toContain(`Welcome to ${projectConstants.projectName}`);
  });

  // @TODO: Opening modals in unit testing is broken since carbon.
  // Need to fix it properly
  it.skip('should show confirmation modal when cluster creation is skipped', () => {
    component.skipClusterCreation();
    expect(modalServiceShowSpy.calls.any()).toBeTruthy();
    expect(modalServiceShowSpy.calls.first().args[0]).toBe(ConfirmationModalComponent);
  });

  it('should show the tearsheet when cluster creation is started', () => {
    openTearsheet();
    const nativeEl = fixture.debugElement.nativeElement;
    expect(nativeEl.querySelector('cd-tearsheet')).not.toBe(null);
  });

  it('should have Add Hosts step component when cluster creation is started', () => {
    openTearsheet();
    const nativeEl = fixture.debugElement.nativeElement;
    expect(nativeEl.querySelector('cd-create-cluster-step-1')).not.toBe(null);
  });

  it('should show the host list in the first step', () => {
    openTearsheet();
    const nativeEl = fixture.debugElement.nativeElement;
    expect(nativeEl.querySelector('cd-hosts')).not.toBe(null);
  });

  it('should ensure osd creation did not happen when no devices are selected', () => {
    component.simpleDeployment = false;
    const osdServiceSpy = spyOn(osdService, 'create').and.callThrough();
    component.onSubmit();
    fixture.detectChanges();
    expect(osdServiceSpy).toBeCalledTimes(0);
  });

  it('should ensure osd creation did happen when devices are selected', () => {
    const osdServiceSpy = spyOn(osdService, 'create').and.callThrough();
    osdService.osdDevices['totalDevices'] = 1;
    component.onSubmit();
    fixture.detectChanges();
    expect(osdServiceSpy).toBeCalledTimes(1);
  });

  it('should ensure host list call happened', () => {
    const hostServiceSpy = spyOn(hostService, 'list').and.callThrough();
    component.onSubmit();
    expect(hostServiceSpy).toHaveBeenCalledTimes(1);
  });

  it('should fire cluster submit when tearsheet Add Storage is clicked on review step', () => {
    const submitSpy = spyOn(component, 'onSubmit').and.callThrough();
    const hostServiceSpy = spyOn(hostService, 'list').and.callThrough();

    openTearsheet();
    component.onSubmit();
    fixture.detectChanges();

    expect(submitSpy).toHaveBeenCalled();
    expect(hostServiceSpy).toHaveBeenCalled();
  });

  it('should show skip button in the Create OSDs step', () => {
    const stepFixture = TestBed.createComponent(CreateClusterStep2Component);
    stepFixture.detectChanges();
    const skipBtn = stepFixture.debugElement.query(By.css('#skipStepBtn')).nativeElement;
    expect(skipBtn).not.toBe(null);
    expect(skipBtn.innerHTML).toBe('Skip');
  });

  it('should skip the Create OSDs step', () => {
    openTearsheet();
    spyOn(component.tearsheet, 'onNext');

    component.onSkipOsdStep();
    fixture.detectChanges();

    expect(component.stepsToSkip['Create OSDs']).toBe(true);
    expect(component.tearsheet.onNext).toHaveBeenCalled();
  });
});

import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { SimpleChange } from '@angular/core';

import { ToastrModule } from 'ngx-toastr';

import { NgbActiveModal, NgbTypeaheadModule } from '@ng-bootstrap/ng-bootstrap';

import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { SharedModule } from '~/app/shared/shared.module';
import { NvmeofSubsystemsStepOneComponent } from './nvmeof-subsystem-step-1.component';
import { FormHelper } from '~/testing/unit-test-helper';
import { NvmeofService } from '~/app/shared/api/nvmeof.service';
import { ComboBoxModule, GridModule, InputModule } from 'carbon-components-angular';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';

import { of } from 'rxjs';

describe('NvmeofSubsystemsStepOneComponent', () => {
  let component: NvmeofSubsystemsStepOneComponent;
  let fixture: ComponentFixture<NvmeofSubsystemsStepOneComponent>;

  let nvmeofService: NvmeofService;

  let form: CdFormGroup;
  let formHelper: FormHelper;
  const mockGroupName = 'default';

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [NvmeofSubsystemsStepOneComponent],
      imports: [
        HttpClientTestingModule,
        SharedModule,
        ReactiveFormsModule,
        RouterTestingModule,
        NgbTypeaheadModule,
        InputModule,
        GridModule,
        ComboBoxModule,
        ToastrModule.forRoot()
      ],
      providers: [NgbActiveModal]
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(NvmeofSubsystemsStepOneComponent);
    component = fixture.componentInstance;

    nvmeofService = TestBed.inject(NvmeofService);
    spyOn(nvmeofService, 'getHostsForGroup').and.returnValue(of([]));
    spyOn(nvmeofService, 'listListeners').and.returnValue(of([]));
    component.group = mockGroupName;
    component.ngOnInit();
    form = component.formGroup;
    formHelper = new FormHelper(form);
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('should test form', () => {
    beforeEach(() => {
      nvmeofService = TestBed.inject(NvmeofService);
      spyOn(nvmeofService, 'createSubsystem').and.stub();
    });

    it('should give error on invalid nqn', () => {
      formHelper.setValue('nqn', 'nqn:2001-07.com.ceph:');
      formHelper.expectError('nqn', 'nqnPattern');
    });
  });

  it('should fetch hosts when the group input becomes available after init', () => {
    const lateService = {
      getHostsForGroup: jest.fn(),
      listListeners: jest.fn()
    } as unknown as NvmeofService;
    const hosts = [{ hostname: 'gw-a', addr: '10.0.0.1' }] as any;
    const lateComponent = new NvmeofSubsystemsStepOneComponent(
      new ActionLabelsI18n(),
      new NgbActiveModal(),
      lateService
    );

    (lateService.getHostsForGroup as jest.Mock).mockReturnValue(of(hosts));
    (lateService.listListeners as jest.Mock).mockReturnValue(of([]));

    lateComponent.ngOnInit();
    expect(lateService.getHostsForGroup).not.toHaveBeenCalled();

    lateComponent.group = mockGroupName;
    lateComponent.ngOnChanges({
      group: new SimpleChange(undefined, mockGroupName, false)
    });

    expect(lateService.getHostsForGroup).toHaveBeenCalledWith(mockGroupName);
    expect(lateComponent.hosts).toEqual([{ content: 'gw-a', addr: '10.0.0.1' }]);
  });

  it('should auto-fetch listeners once listener page inputs are ready', () => {
    const lateService = {
      getHostsForGroup: jest.fn(),
      listListeners: jest.fn()
    } as unknown as NvmeofService;
    const hosts = [
      { hostname: 'gw-a', addr: '10.0.0.1' },
      { hostname: 'gw-b', addr: '10.0.0.2' }
    ] as any;
    const existingListeners = [{ host_name: 'gw-a' }];
    const lateComponent = new NvmeofSubsystemsStepOneComponent(
      new ActionLabelsI18n(),
      new NgbActiveModal(),
      lateService
    );

    lateComponent.listenersOnly = true;
    (lateService.getHostsForGroup as jest.Mock).mockReturnValue(of(hosts));
    (lateService.listListeners as jest.Mock).mockReturnValue(of(existingListeners));

    lateComponent.ngOnInit();
    expect(lateService.getHostsForGroup).not.toHaveBeenCalled();
    expect(lateService.listListeners).not.toHaveBeenCalled();

    lateComponent.group = mockGroupName;
    lateComponent.subsystemNQN = 'nqn.2001-07.com.ceph:listener-test';
    lateComponent.ngOnChanges({
      group: new SimpleChange(undefined, mockGroupName, false),
      subsystemNQN: new SimpleChange(undefined, lateComponent.subsystemNQN, false)
    });

    expect(lateService.getHostsForGroup).toHaveBeenCalledWith(mockGroupName);
    expect(lateService.listListeners).toHaveBeenCalledWith(
      'nqn.2001-07.com.ceph:listener-test',
      mockGroupName
    );
    expect(lateComponent.hosts).toEqual([{ content: 'gw-b', addr: '10.0.0.2' }]);
  });
});

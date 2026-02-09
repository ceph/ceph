import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ToastrModule } from 'ngx-toastr';

import { NgbActiveModal, NgbTypeaheadModule } from '@ng-bootstrap/ng-bootstrap';

import { SharedModule } from '~/app/shared/shared.module';
import {
  NvmeofSubsystemsFormComponent,
  SubsystemPayload
} from './nvmeof-subsystems-form.component';
import { NvmeofService } from '~/app/shared/api/nvmeof.service';
import { NvmeofSubsystemsStepOneComponent } from './nvmeof-subsystem-step-1/nvmeof-subsystem-step-1.component';
import { GridModule, InputModule, RadioModule, TagModule } from 'carbon-components-angular';
import { NvmeofSubsystemsStepThreeComponent } from './nvmeof-subsystem-step-3/nvmeof-subsystem-step-3.component';
import { HOST_TYPE } from '~/app/shared/models/nvmeof';
import { NvmeofSubsystemsStepTwoComponent } from './nvmeof-subsystem-step-2/nvmeof-subsystem-step-2.component';
import { of } from 'rxjs';

describe('NvmeofSubsystemsFormComponent', () => {
  let component: NvmeofSubsystemsFormComponent;
  let fixture: ComponentFixture<NvmeofSubsystemsFormComponent>;
  let nvmeofService: NvmeofService;
  const mockTimestamp = 1720693470789;
  const mockGroupName = 'default';
  const mockPayload: SubsystemPayload = {
    nqn: '',
    gw_group: mockGroupName,
    subsystemDchapKey: 'Q2VwaE52bWVvRkNoYXBTeW50aGV0aWNLZXkxMjM0NTY=',
    addedHosts: [],
    hostType: HOST_TYPE.ALL
  };

  beforeEach(async () => {
    spyOn(Date, 'now').and.returnValue(mockTimestamp);
    await TestBed.configureTestingModule({
      declarations: [
        NvmeofSubsystemsFormComponent,
        NvmeofSubsystemsStepOneComponent,
        NvmeofSubsystemsStepThreeComponent,
        NvmeofSubsystemsStepTwoComponent
      ],
      providers: [NgbActiveModal],
      imports: [
        HttpClientTestingModule,
        NgbTypeaheadModule,
        ReactiveFormsModule,
        RouterTestingModule,
        SharedModule,
        InputModule,
        GridModule,
        RadioModule,
        TagModule,
        ToastrModule.forRoot()
      ]
    }).compileComponents();

    fixture = TestBed.createComponent(NvmeofSubsystemsFormComponent);
    component = fixture.componentInstance;
    component.ngOnInit();
    fixture.detectChanges();
    component.group = mockGroupName;
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('should test form', () => {
    beforeEach(() => {
      nvmeofService = TestBed.inject(NvmeofService);
      spyOn(nvmeofService, 'createSubsystem').and.returnValue(of({}));
      spyOn(nvmeofService, 'addSubsystemInitiators').and.returnValue(of({}));
    });

    it('should be creating request correctly', () => {
      const expectedNqn = 'nqn.2001-07.com.ceph:' + mockTimestamp;
      mockPayload['nqn'] = expectedNqn;
      component.onSubmit(mockPayload);
      expect(nvmeofService.createSubsystem).toHaveBeenCalledWith({
        nqn: expectedNqn,
        gw_group: mockGroupName,
        enable_ha: true,
        dhchap_key: 'Q2VwaE52bWVvRkNoYXBTeW50aGV0aWNLZXkxMjM0NTY='
      });
    });

    it('should add initiators with wildcard when hostType is ALL', () => {
      const payload: SubsystemPayload = {
        nqn: 'test-nqn',
        gw_group: mockGroupName,
        addedHosts: [],
        hostType: HOST_TYPE.ALL,
        subsystemDchapKey: 'Q2VwaE52bWVvRkNoYXBTeW50aGV0aWNLZXkxMjM0NTY='
      };

      component.group = mockGroupName;
      component.onSubmit(payload);

      expect(nvmeofService.addSubsystemInitiators).toHaveBeenCalledWith('test-nqn.default', {
        host_nqn: '*',
        gw_group: mockGroupName
      });
    });
  });
});

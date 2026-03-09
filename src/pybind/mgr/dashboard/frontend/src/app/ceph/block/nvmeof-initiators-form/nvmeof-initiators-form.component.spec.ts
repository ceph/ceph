import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ToastrModule } from 'ngx-toastr';

import { NgbActiveModal, NgbTypeaheadModule } from '@ng-bootstrap/ng-bootstrap';

import { SharedModule } from '~/app/shared/shared.module';
import { NvmeofService } from '~/app/shared/api/nvmeof.service';

import { NvmeofInitiatorsFormComponent } from './nvmeof-initiators-form.component';

describe('NvmeofInitiatorsFormComponent', () => {
  let component: NvmeofInitiatorsFormComponent;
  let fixture: ComponentFixture<NvmeofInitiatorsFormComponent>;
  let nvmeofService: NvmeofService;
  const mockTimestamp = 1720693470789;

  beforeEach(async () => {
    spyOn(Date, 'now').and.returnValue(mockTimestamp);
    await TestBed.configureTestingModule({
      declarations: [NvmeofInitiatorsFormComponent],
      providers: [NgbActiveModal],
      imports: [
        HttpClientTestingModule,
        NgbTypeaheadModule,
        ReactiveFormsModule,
        RouterTestingModule,
        SharedModule,
        ToastrModule.forRoot()
      ]
    }).compileComponents();

    fixture = TestBed.createComponent(NvmeofInitiatorsFormComponent);
    component = fixture.componentInstance;
    component.ngOnInit();
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should initialize with two steps (Host access control + Authentication optional)', () => {
    expect(component.steps.length).toBe(2);
    expect(component.steps[0].label).toBe('Host access control');
    expect(component.steps[1].label).toBe('Authentication (optional)');
  });

  it('should hide Authentication step when showAuthStep is false', () => {
    component.showAuthStep = false;
    component.rebuildSteps();
    expect(component.steps.length).toBe(1);
    expect(component.steps[0].label).toBe('Host access control');
  });

  describe('should test form', () => {
    beforeEach(() => {
      nvmeofService = TestBed.inject(NvmeofService);
      spyOn(nvmeofService, 'addInitiators').and.stub();
    });

    it('should be creating request correctly', () => {
      const subsystemNQN = 'nqn.2001-07.com.ceph:' + mockTimestamp;
      component.subsystemNQN = subsystemNQN;
      component.onSubmit();
      expect(nvmeofService.addInitiators).toHaveBeenCalledWith(subsystemNQN, {
        host_nqn: ''
      });
    });

    it('should build hosts from addedHosts when hostDchapKeyList is absent', () => {
      const subsystemNQN = 'nqn.test';
      component.subsystemNQN = subsystemNQN;
      component.group = 'test-group';

      const payload: any = {
        hostType: HOST_TYPE.SPECIFIC,
        addedHosts: ['host2'],
        gw_group: 'test-group'
      };

      component.onSubmit(payload);
      expect(nvmeofService.addSubsystemInitiators).toHaveBeenCalledWith(subsystemNQN, {
        allow_all: false,
        gw_group: 'test-group',
        hosts: [{ dhchap_key: '', host_nqn: 'host2' }]
      });
    });
  });
});

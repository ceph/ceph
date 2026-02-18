import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ActivatedRoute } from '@angular/router';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { of } from 'rxjs';

import { ToastrModule } from 'ngx-toastr';

import { NgbActiveModal, NgbTypeaheadModule } from '@ng-bootstrap/ng-bootstrap';

import { SharedModule } from '~/app/shared/shared.module';
import { NvmeofService } from '~/app/shared/api/nvmeof.service';
import { HOST_TYPE } from '~/app/shared/models/nvmeof';

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
      schemas: [NO_ERRORS_SCHEMA],
      providers: [
        NgbActiveModal,
        {
          provide: ActivatedRoute,
          useValue: {
            queryParams: of({ group: 'test-group' }),
            params: of({ subsystem_nqn: 'nqn.test' }),
            parent: {
              params: of({ subsystem_nqn: 'nqn.test' })
            }
          }
        }
      ],
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

  describe('should test form', () => {
    beforeEach(() => {
      nvmeofService = TestBed.inject(NvmeofService);
      spyOn(nvmeofService, 'addInitiators').and.stub();
    });

    it('should be creating request correctly', () => {
      const subsystemNQN = 'nqn.test';
      component.subsystemNQN = subsystemNQN;
      component.group = 'test-group';

      const payload: any = {
        hostType: HOST_TYPE.SPECIFIC,
        addedHosts: ['host1']
      };

      component.onSubmit(payload);
      expect(nvmeofService.addInitiators).toHaveBeenCalledWith(subsystemNQN, {
        host_nqn: 'host1',
        gw_group: 'test-group'
      });
    });
  });
});

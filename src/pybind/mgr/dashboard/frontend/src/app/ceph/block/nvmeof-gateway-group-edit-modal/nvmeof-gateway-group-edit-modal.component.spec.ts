import { ComponentFixture, TestBed } from '@angular/core/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ReactiveFormsModule } from '@angular/forms';

import { NvmeofGatewayGroupEditModalComponent } from './nvmeof-gateway-group-edit-modal.component';
import { SharedModule } from '~/app/shared/shared.module';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { TaskMessageService } from '~/app/shared/services/task-message.service';

describe('NvmeofGatewayGroupEditModalComponent', () => {
  let component: NvmeofGatewayGroupEditModalComponent;
  let fixture: ComponentFixture<NvmeofGatewayGroupEditModalComponent>;

  const mockGatewayGroup = {
    service_name: 'nvmeof.rbd.gateway-prod',
    spec: {
      service_type: 'nvmeof',
      service_id: 'rbd.gateway-prod',
      group: 'gateway-prod',
      pool: 'rbd',
      enable_auth: false,
      enable_mtls: false
    },
    status: {
      running: 2,
      size: 2
    }
  };

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [NvmeofGatewayGroupEditModalComponent],
      imports: [HttpClientTestingModule, ReactiveFormsModule, SharedModule],
      providers: [
        ModalCdsService,
        TaskMessageService,
        { provide: 'gatewayGroup', useValue: mockGatewayGroup }
      ]
    }).compileComponents();

    fixture = TestBed.createComponent(NvmeofGatewayGroupEditModalComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should initialize form with gateway group data', () => {
    expect(component.editForm.get('groupName')?.value).toEqual('gateway-prod');
    expect(component.editForm.get('enableEncryption')?.value).toEqual(false);
    expect(component.editForm.get('enableMtls')?.value).toEqual(false);
  });

  it('should disable groupName field', () => {
    expect(component.editForm.get('groupName')?.disabled).toEqual(true);
  });
});

// Made with Bob

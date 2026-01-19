import { ComponentFixture, TestBed } from '@angular/core/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { NvmeofGatewayNodeAddModalComponent } from './nvmeof-gateway-node-add-modal.component';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';

import { RouterTestingModule } from '@angular/router/testing';

describe('NvmeofGatewayNodeAddModalComponent', () => {
  let component: NvmeofGatewayNodeAddModalComponent;
  let fixture: ComponentFixture<NvmeofGatewayNodeAddModalComponent>;

  configureTestBed({
    imports: [SharedModule, HttpClientTestingModule, RouterTestingModule],
    declarations: [NvmeofGatewayNodeAddModalComponent]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(NvmeofGatewayNodeAddModalComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});

import { HttpClientTestingModule } from '@angular/common/http/testing';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { NgbNavModule } from '@ng-bootstrap/ng-bootstrap';
import { Permissions } from '~/app/shared/models/permissions';
import { SharedModule } from '~/app/shared/shared.module';
import { NvmeofSubsystemsDetailsComponent } from './nvmeof-subsystems-details.component';

describe('NvmeofSubsystemsDetailsComponent', () => {
  let component: NvmeofSubsystemsDetailsComponent;
  let fixture: ComponentFixture<NvmeofSubsystemsDetailsComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [NvmeofSubsystemsDetailsComponent],
      imports: [BrowserAnimationsModule, SharedModule, HttpClientTestingModule, NgbNavModule]
    }).compileComponents();

    fixture = TestBed.createComponent(NvmeofSubsystemsDetailsComponent);
    component = fixture.componentInstance;
    component.selection = {
      serial_number: 'Ceph30487186726692',
      model_number: 'Ceph bdev Controller',
      min_cntlid: 1,
      max_cntlid: 2040,
      subtype: 'NVMe',
      nqn: 'nqn.2001-07.com.ceph:1720603703820',
      namespace_count: 1,
      max_namespaces: 256
    };
    component.permissions = new Permissions({
      grafana: ['read']
    });
    component.ngOnChanges();
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should prepare data', () => {
    expect(component.data).toEqual({
      'Serial Number': 'Ceph30487186726692',
      'Model Number': 'Ceph bdev Controller',
      'Minimum Controller Identifier': 1,
      'Maximum Controller Identifier': 2040,
      'Subsystem Type': 'NVMe'
    });
  });
});

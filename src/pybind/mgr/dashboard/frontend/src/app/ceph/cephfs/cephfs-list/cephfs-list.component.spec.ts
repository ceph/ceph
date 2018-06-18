import { HttpClientTestingModule } from '@angular/common/http/testing';
import { Component, Input } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { CephfsService } from '../../../shared/api/cephfs.service';
import { CdTableSelection } from '../../../shared/models/cd-table-selection';
import { SharedModule } from '../../../shared/shared.module';
import { configureTestBed } from '../../../shared/unit-test-helper';
import { CephfsListComponent } from './cephfs-list.component';

@Component({ selector: 'cd-cephfs-detail', template: '' })
class CephfsDetailStubComponent {
  @Input() selection: CdTableSelection;
}

describe('CephfsListComponent', () => {
  let component: CephfsListComponent;
  let fixture: ComponentFixture<CephfsListComponent>;

  configureTestBed({
    imports: [SharedModule, HttpClientTestingModule],
    declarations: [CephfsListComponent, CephfsDetailStubComponent],
    providers: [CephfsService]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(CephfsListComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});

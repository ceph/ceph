import { HttpClientTestingModule } from '@angular/common/http/testing';
import { Component, Input } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { RouterTestingModule } from '@angular/router/testing';

import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { CephfsVolumeFormComponent } from '../cephfs-form/cephfs-form.component';
import { CephfsListComponent } from './cephfs-list.component';

@Component({ selector: 'cd-cephfs-tabs', template: '' })
class CephfsTabsStubComponent {
  @Input()
  selection: CdTableSelection;
}

describe('CephfsListComponent', () => {
  let component: CephfsListComponent;
  let fixture: ComponentFixture<CephfsListComponent>;

  configureTestBed({
    imports: [BrowserAnimationsModule, SharedModule, HttpClientTestingModule, RouterTestingModule],
    declarations: [CephfsListComponent, CephfsTabsStubComponent, CephfsVolumeFormComponent]
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

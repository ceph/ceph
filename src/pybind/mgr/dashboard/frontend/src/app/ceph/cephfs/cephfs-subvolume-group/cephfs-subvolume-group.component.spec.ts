import { ComponentFixture, TestBed } from '@angular/core/testing';

import { CephfsSubvolumeGroupComponent } from './cephfs-subvolume-group.component';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ToastrModule } from 'ngx-toastr';
import { RouterTestingModule } from '@angular/router/testing';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';

describe('CephfsSubvolumeGroupComponent', () => {
  let component: CephfsSubvolumeGroupComponent;
  let fixture: ComponentFixture<CephfsSubvolumeGroupComponent>;

  configureTestBed({
    declarations: [CephfsSubvolumeGroupComponent],
    imports: [HttpClientTestingModule, SharedModule, ToastrModule.forRoot(), RouterTestingModule]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(CephfsSubvolumeGroupComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});

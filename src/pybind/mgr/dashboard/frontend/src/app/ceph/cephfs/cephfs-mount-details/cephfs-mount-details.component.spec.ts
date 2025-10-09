import { ComponentFixture, TestBed } from '@angular/core/testing';

import { CephfsMountDetailsComponent } from './cephfs-mount-details.component';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { SharedModule } from '~/app/shared/shared.module';
import { ToastrModule } from 'ngx-toastr';
import { RouterTestingModule } from '@angular/router/testing';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { configureTestBed } from '~/testing/unit-test-helper';

describe('CephfsSnapshotscheduleListComponent', () => {
  let component: CephfsMountDetailsComponent;
  let fixture: ComponentFixture<CephfsMountDetailsComponent>;

  configureTestBed({
    declarations: [CephfsMountDetailsComponent],
    imports: [HttpClientTestingModule, SharedModule, ToastrModule.forRoot(), RouterTestingModule],
    providers: [NgbActiveModal]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(CephfsMountDetailsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});

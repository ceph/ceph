import { ComponentFixture, TestBed } from '@angular/core/testing';

import { CephfsSubvolumegroupFormComponent } from './cephfs-subvolumegroup-form.component';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { ToastrModule } from 'ngx-toastr';
import { SharedModule } from '~/app/shared/shared.module';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';
import { configureTestBed } from '~/testing/unit-test-helper';

describe('CephfsSubvolumegroupFormComponent', () => {
  let component: CephfsSubvolumegroupFormComponent;
  let fixture: ComponentFixture<CephfsSubvolumegroupFormComponent>;

  configureTestBed({
    declarations: [CephfsSubvolumegroupFormComponent],
    providers: [NgbActiveModal],
    imports: [
      SharedModule,
      ToastrModule.forRoot(),
      ReactiveFormsModule,
      HttpClientTestingModule,
      RouterTestingModule
    ]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(CephfsSubvolumegroupFormComponent);
    component = fixture.componentInstance;
    component.pools = [];
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});

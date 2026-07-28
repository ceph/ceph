import { ComponentFixture, TestBed } from '@angular/core/testing';

import { CephfsAuthModalComponent } from './cephfs-auth-modal.component';
import { NgbTypeaheadModule } from '@ng-bootstrap/ng-bootstrap';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { SharedModule } from '~/app/shared/shared.module';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';
import { CheckboxModule, InputModule, ModalModule } from 'carbon-components-angular';
import { configureTestBed } from '~/testing/unit-test-helper';

describe('CephfsAuthModalComponent', () => {
  let component: CephfsAuthModalComponent;
  let fixture: ComponentFixture<CephfsAuthModalComponent>;

  configureTestBed({
    declarations: [CephfsAuthModalComponent],
    imports: [
      HttpClientTestingModule,
      SharedModule,
      ReactiveFormsModule,
      RouterTestingModule,
      NgbTypeaheadModule,
      ModalModule,
      InputModule,
      CheckboxModule
    ]
  });

  beforeEach(async () => {
    fixture = TestBed.createComponent(CephfsAuthModalComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});

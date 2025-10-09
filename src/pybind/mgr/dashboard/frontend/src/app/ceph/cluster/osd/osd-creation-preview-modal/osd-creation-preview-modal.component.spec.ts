import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { ToastrModule } from 'ngx-toastr';

import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { OsdCreationPreviewModalComponent } from './osd-creation-preview-modal.component';

describe('OsdCreationPreviewModalComponent', () => {
  let component: OsdCreationPreviewModalComponent;
  let fixture: ComponentFixture<OsdCreationPreviewModalComponent>;

  configureTestBed({
    imports: [
      HttpClientTestingModule,
      ReactiveFormsModule,
      SharedModule,
      RouterTestingModule,
      ToastrModule.forRoot()
    ],
    providers: [NgbActiveModal],
    declarations: [OsdCreationPreviewModalComponent]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(OsdCreationPreviewModalComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});

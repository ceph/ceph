import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { RouterTestingModule } from '@angular/router/testing';

import { BsModalRef } from 'ngx-bootstrap/modal';
import { TabsModule } from 'ngx-bootstrap/tabs';
import { ToastrModule } from 'ngx-toastr';

import { configureTestBed, i18nProviders } from '../../../../../testing/unit-test-helper';
import { SharedModule } from '../../../../shared/shared.module';
import { OsdCreationPreviewDetailsComponent } from '../osd-creation-preview-details/osd-creation-preview-details.component';
import { OsdCreationPreviewModalComponent } from './osd-creation-preview-modal.component';

describe('OsdCreationPreviewModalComponent', () => {
  let component: OsdCreationPreviewModalComponent;
  let fixture: ComponentFixture<OsdCreationPreviewModalComponent>;

  configureTestBed({
    imports: [
      BrowserAnimationsModule,
      HttpClientTestingModule,
      ReactiveFormsModule,
      SharedModule,
      RouterTestingModule,
      ToastrModule.forRoot(),
      TabsModule.forRoot()
    ],
    providers: [BsModalRef, i18nProviders],
    declarations: [OsdCreationPreviewModalComponent, OsdCreationPreviewDetailsComponent]
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

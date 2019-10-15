import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';

import { BsModalRef } from 'ngx-bootstrap/modal';

import { configureTestBed, i18nProviders } from '../../../../../testing/unit-test-helper';
import { SharedModule } from '../../../../shared/shared.module';
import { DriveGroup } from '../osd-form/drive-group.model';
import { OsdCreationPreviewModalComponent } from './osd-creation-preview-modal.component';

describe('OsdCreationPreviewModalComponent', () => {
  let component: OsdCreationPreviewModalComponent;
  let fixture: ComponentFixture<OsdCreationPreviewModalComponent>;

  configureTestBed({
    imports: [HttpClientTestingModule, ReactiveFormsModule, SharedModule, RouterTestingModule],
    providers: [BsModalRef, i18nProviders],
    declarations: [OsdCreationPreviewModalComponent]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(OsdCreationPreviewModalComponent);
    component = fixture.componentInstance;
    component.driveGroup = new DriveGroup();
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});

import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';

import { configureTestBed, i18nProviders } from '../../../../testing/unit-test-helper';
import { SharedModule } from '../../../shared/shared.module';
import { RgwUserS3KeyModalComponent } from './rgw-user-s3-key-modal.component';

describe('RgwUserS3KeyModalComponent', () => {
  let component: RgwUserS3KeyModalComponent;
  let fixture: ComponentFixture<RgwUserS3KeyModalComponent>;

  configureTestBed({
    declarations: [RgwUserS3KeyModalComponent],
    imports: [ReactiveFormsModule, SharedModule, RouterTestingModule],
    providers: [NgbActiveModal, i18nProviders]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RgwUserS3KeyModalComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});

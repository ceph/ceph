import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';

import { SharedModule } from '~/app/shared/shared.module';

import { RgwMultisiteImportComponent } from './rgw-multisite-import.component';
import { configureTestBed } from '~/testing/unit-test-helper';

describe('RgwMultisiteImportComponent', () => {
  let component: RgwMultisiteImportComponent;
  let fixture: ComponentFixture<RgwMultisiteImportComponent>;

  configureTestBed({
    imports: [SharedModule, ReactiveFormsModule, RouterTestingModule, HttpClientTestingModule],
    declarations: [RgwMultisiteImportComponent],
    providers: [NgbActiveModal]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RgwMultisiteImportComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});

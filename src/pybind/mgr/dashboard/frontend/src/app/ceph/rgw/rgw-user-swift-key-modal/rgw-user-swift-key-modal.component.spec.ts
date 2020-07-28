import { ComponentFixture, TestBed } from '@angular/core/testing';
import { FormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { ToastrModule } from 'ngx-toastr';

import { configureTestBed } from '../../../../testing/unit-test-helper';
import { SharedModule } from '../../../shared/shared.module';
import { RgwUserSwiftKeyModalComponent } from './rgw-user-swift-key-modal.component';

describe('RgwUserSwiftKeyModalComponent', () => {
  let component: RgwUserSwiftKeyModalComponent;
  let fixture: ComponentFixture<RgwUserSwiftKeyModalComponent>;

  configureTestBed({
    declarations: [RgwUserSwiftKeyModalComponent],
    imports: [ToastrModule.forRoot(), FormsModule, SharedModule, RouterTestingModule],
    providers: [NgbActiveModal]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RgwUserSwiftKeyModalComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});

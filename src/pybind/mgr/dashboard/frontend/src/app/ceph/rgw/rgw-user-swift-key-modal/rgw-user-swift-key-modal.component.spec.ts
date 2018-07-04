import { ComponentFixture, TestBed } from '@angular/core/testing';
import { FormsModule } from '@angular/forms';

import { BsModalRef } from 'ngx-bootstrap/modal';

import { configureTestBed } from '../../../../testing/unit-test-helper';
import { RgwUserSwiftKeyModalComponent } from './rgw-user-swift-key-modal.component';

describe('RgwUserSwiftKeyModalComponent', () => {
  let component: RgwUserSwiftKeyModalComponent;
  let fixture: ComponentFixture<RgwUserSwiftKeyModalComponent>;

  configureTestBed({
    declarations: [RgwUserSwiftKeyModalComponent],
    imports: [FormsModule],
    providers: [BsModalRef]
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

import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { FormsModule } from '@angular/forms';

import { BsModalRef } from 'ngx-bootstrap/modal';

import { RgwUserSwiftKeyModalComponent } from './rgw-user-swift-key-modal.component';

describe('RgwUserSwiftKeyModalComponent', () => {
  let component: RgwUserSwiftKeyModalComponent;
  let fixture: ComponentFixture<RgwUserSwiftKeyModalComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ RgwUserSwiftKeyModalComponent ],
      imports: [
        FormsModule
      ],
      providers: [ BsModalRef ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(RgwUserSwiftKeyModalComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});

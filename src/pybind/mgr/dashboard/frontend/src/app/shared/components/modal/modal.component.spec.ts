import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ModalModule } from 'ngx-bootstrap/modal';

import { configureTestBed } from '../../unit-test-helper';
import { ModalComponent } from './modal.component';

describe('ModalComponent', () => {
  let component: ModalComponent;
  let fixture: ComponentFixture<ModalComponent>;

  configureTestBed({
    imports: [ModalModule.forRoot()],
    declarations: [ModalComponent]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ModalComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});

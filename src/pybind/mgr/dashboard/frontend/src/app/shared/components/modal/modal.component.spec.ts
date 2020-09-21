import { ComponentFixture, TestBed } from '@angular/core/testing';

import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';

import { configureTestBed } from '../../../../testing/unit-test-helper';
import { ModalComponent } from './modal.component';

describe('ModalComponent', () => {
  let component: ModalComponent;
  let fixture: ComponentFixture<ModalComponent>;

  configureTestBed({
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

  it('should call the hide callback function', () => {
    spyOn(component.hide, 'emit');
    const nativeElement = fixture.nativeElement;
    const button = nativeElement.querySelector('button');
    button.dispatchEvent(new Event('click'));
    fixture.detectChanges();
    expect(component.hide.emit).toHaveBeenCalled();
  });

  it('should hide the modal', () => {
    component.modalRef = new NgbActiveModal();
    spyOn(component.modalRef, 'close');
    component.close();
    expect(component.modalRef.close).toHaveBeenCalled();
  });
});

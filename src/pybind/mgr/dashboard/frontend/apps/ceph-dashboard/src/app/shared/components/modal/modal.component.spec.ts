import { ComponentFixture, TestBed } from '@angular/core/testing';
import { Router } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';

import { configureTestBed } from '~/testing/unit-test-helper';
import { ModalComponent } from './modal.component';

describe('ModalComponent', () => {
  let component: ModalComponent;
  let fixture: ComponentFixture<ModalComponent>;
  let routerNavigateSpy: jasmine.Spy;

  configureTestBed({
    declarations: [ModalComponent],
    imports: [RouterTestingModule]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ModalComponent);
    component = fixture.componentInstance;
    routerNavigateSpy = spyOn(TestBed.inject(Router), 'navigate');
    routerNavigateSpy.and.returnValue(true);
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

  it('should hide the routed modal', () => {
    component.pageURL = 'hosts';
    component.close();
    expect(routerNavigateSpy).toHaveBeenCalledTimes(1);
    expect(routerNavigateSpy).toHaveBeenCalledWith(['hosts', { outlets: { modal: null } }]);
  });
});

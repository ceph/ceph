import { Component, ViewChild } from '@angular/core';
import { async, ComponentFixture, fakeAsync, TestBed, tick } from '@angular/core/testing';
import { FormGroupDirective, ReactiveFormsModule } from '@angular/forms';

import { ModalModule } from 'ngx-bootstrap';
import { Observable } from 'rxjs/Observable';
import { Subscriber } from 'rxjs/Subscriber';

import { ModalComponent } from '../modal/modal.component';
import { SubmitButtonComponent } from '../submit-button/submit-button.component';
import { DeletionButtonComponent } from './deletion-button.component';

@Component({
  template: `
    <cd-deletion-button #ctrlDeleteButton
                        metaType="Controller delete handling"
                        pattern="ctrl-test"
                        (toggleDeletion)="fakeDeleteController()">
      The spinner is handled by the controller if you have use the modal as ViewChild in order to
      use it's functions to stop the spinner or close the dialog.
    </cd-deletion-button>
    <cd-deletion-button #modalDeleteButton
                        metaType="Modal delete handling"
                        [deletionObserver]="fakeDelete()"
                        pattern="modal-test">
      The spinner is handled by the modal if your given deletion function returns a Observable.
    </cd-deletion-button>
  `
})
class MockComponent {
  @ViewChild('ctrlDeleteButton') ctrlDeleteButton: DeletionButtonComponent;
  @ViewChild('modalDeleteButton') modalDeleteButton: DeletionButtonComponent;
  someData = [1, 2, 3, 4, 5];
  finished: number[];

  finish() {
    this.finished = [6, 7, 8, 9];
  }

  fakeDelete() {
    return (): Observable<any> => {
      return new Observable((observer: Subscriber<any>) => {
        Observable.timer(100).subscribe(() => {
          observer.next(this.finish());
          observer.complete();
        });
      });
    };
  }

  fakeDeleteController() {
    Observable.timer(100).subscribe(() => {
      this.finish();
      this.ctrlDeleteButton.hideModal();
    });
  }
}

describe('DeletionButtonComponent', () => {
  let mockComponent: MockComponent;
  let component: DeletionButtonComponent;
  let fixture: ComponentFixture<MockComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ MockComponent, DeletionButtonComponent, ModalComponent,
        SubmitButtonComponent],
      imports: [ModalModule.forRoot(), ReactiveFormsModule],
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(MockComponent);
    mockComponent = fixture.componentInstance;
    component = mockComponent.ctrlDeleteButton;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('component functions', () => {

    const mockShowModal = () => {
      component.showModal(null);
    };

    const changeValue = (value) => {
      component.confirmation.setValue(value);
      component.confirmation.markAsDirty();
      component.confirmation.updateValueAndValidity();
    };

    beforeEach(() => {
      spyOn(component.modalService, 'show').and.returnValue({
        hide: () => true
      });
    });

    it('should test showModal', () => {
      changeValue('something');
      expect(mockShowModal).toBeTruthy();
      expect(component.confirmation.value).toBe('something');
      expect(component.modalService.show).not.toHaveBeenCalled();
      mockShowModal();
      expect(component.modalService.show).toHaveBeenCalled();
      expect(component.confirmation.value).toBe(null);
      expect(component.confirmation.pristine).toBe(true);
    });

    it('should test hideModal', () => {
      expect(component.bsModalRef).not.toBeTruthy();
      mockShowModal();
      expect(component.bsModalRef).toBeTruthy();
      expect(component.hideModal).toBeTruthy();
      spyOn(component.bsModalRef, 'hide').and.stub();
      expect(component.bsModalRef.hide).not.toHaveBeenCalled();
      component.hideModal();
      expect(component.bsModalRef.hide).toHaveBeenCalled();
    });

    describe('invalid control', () => {

      const testInvalidControl = (submitted: boolean, error: string, expected: boolean) => {
        expect(component.invalidControl(submitted, error)).toBe(expected);
      };

      beforeEach(() => {
        component.deletionForm.reset();
      });

      it('should test empty values', () => {
        expect(component.invalidControl).toBeTruthy();
        component.deletionForm.reset();
        testInvalidControl(false, undefined, false);
        testInvalidControl(true, 'required', true);
        component.deletionForm.reset();
        changeValue('let-me-pass');
        changeValue('');
        testInvalidControl(true, 'required', true);
      });

      it('should test pattern', () => {
        changeValue('let-me-pass');
        testInvalidControl(false, 'pattern', true);
        changeValue('ctrl-test');
        testInvalidControl(false, undefined, false);
        testInvalidControl(true, undefined, false);
      });
    });

    describe('deletion call', () => {
      beforeEach(() => {
        spyOn(component.toggleDeletion, 'emit');
        spyOn(component, 'stopLoadingSpinner');
        spyOn(component, 'hideModal').and.stub();
      });

      describe('Controller driven', () => {
        beforeEach(() => {
          mockShowModal();
          expect(component.toggleDeletion.emit).not.toHaveBeenCalled();
          expect(component.stopLoadingSpinner).not.toHaveBeenCalled();
          expect(component.hideModal).not.toHaveBeenCalled();
        });

        it('should delete without doing anything but call emit', () => {
          component.deletionCall();
          expect(component.stopLoadingSpinner).not.toHaveBeenCalled();
          expect(component.hideModal).not.toHaveBeenCalled();
          expect(component.toggleDeletion.emit).toHaveBeenCalled();
        });

        it('should test fake deletion that closes modal', <any>fakeAsync(() => {
          mockComponent.fakeDeleteController();
          expect(component.hideModal).not.toHaveBeenCalled();
          expect(mockComponent.finished).toBe(undefined);
          tick(2000);
          expect(component.hideModal).toHaveBeenCalled();
          expect(mockComponent.finished).toEqual([6, 7, 8, 9]);
        }));
      });

      describe('Modal driven', () => {
        it('should delete and close modal', <any>fakeAsync(() => {
          component = mockComponent.modalDeleteButton;
          mockShowModal();
          spyOn(component.toggleDeletion, 'emit');
          spyOn(component, 'stopLoadingSpinner');
          spyOn(component, 'hideModal').and.stub();
          spyOn(mockComponent, 'fakeDelete');

          component.deletionCall();
          expect(mockComponent.finished).toBe(undefined);
          expect(component.toggleDeletion.emit).not.toHaveBeenCalled();
          expect(component.hideModal).not.toHaveBeenCalled();

          tick(2000);
          expect(component.toggleDeletion.emit).not.toHaveBeenCalled();
          expect(component.stopLoadingSpinner).not.toHaveBeenCalled();
          expect(component.hideModal).toHaveBeenCalled();
          expect(mockComponent.finished).toEqual([6, 7, 8, 9]);
        }));
      });
    });
  });

});

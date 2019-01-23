import { Component, NgModule, NO_ERRORS_SCHEMA, TemplateRef, ViewChild } from '@angular/core';
import { ComponentFixture, fakeAsync, TestBed, tick } from '@angular/core/testing';
import { NgForm, ReactiveFormsModule } from '@angular/forms';
import { By } from '@angular/platform-browser';

import { BsModalRef, BsModalService, ModalModule } from 'ngx-bootstrap/modal';
import { Observable, Subscriber, timer as observableTimer } from 'rxjs';

import { configureTestBed } from '../../../../testing/unit-test-helper';
import { DirectivesModule } from '../../directives/directives.module';
import { CriticalConfirmationModalComponent } from './critical-confirmation-modal.component';

@NgModule({
  entryComponents: [CriticalConfirmationModalComponent]
})
export class MockModule {}

@Component({
  template: `
    <button type="button" class="btn btn-sm btn-primary" (click)="openCtrlDriven()">
      <i class="fa fa-fw fa-times"></i>Deletion Ctrl-Test
      <ng-template #ctrlDescription>
        The spinner is handled by the controller if you have use the modal as ViewChild in order to
        use it's functions to stop the spinner or close the dialog.
      </ng-template>
    </button>

    <button type="button" class="btn btn-sm btn-primary" (click)="openModalDriven()">
      <i class="fa fa-fw fa-times"></i>Deletion Modal-Test
      <ng-template #modalDescription>
        The spinner is handled by the modal if your given deletion function returns a Observable.
      </ng-template>
    </button>
  `
})
class MockComponent {
  @ViewChild('ctrlDescription')
  ctrlDescription: TemplateRef<any>;
  @ViewChild('modalDescription')
  modalDescription: TemplateRef<any>;
  someData = [1, 2, 3, 4, 5];
  finished: number[];
  ctrlRef: BsModalRef;
  modalRef: BsModalRef;

  // Normally private - public was needed for the tests
  constructor(public modalService: BsModalService) {}

  openCtrlDriven() {
    this.ctrlRef = this.modalService.show(CriticalConfirmationModalComponent, {
      initialState: {
        submitAction: this.fakeDeleteController.bind(this),
        bodyTemplate: this.ctrlDescription
      }
    });
  }

  openModalDriven() {
    this.modalRef = this.modalService.show(CriticalConfirmationModalComponent, {
      initialState: {
        submitActionObservable: this.fakeDelete(),
        bodyTemplate: this.modalDescription
      }
    });
  }

  finish() {
    this.finished = [6, 7, 8, 9];
  }

  fakeDelete() {
    return (): Observable<any> => {
      return new Observable((observer: Subscriber<any>) => {
        observableTimer(100).subscribe(() => {
          observer.next(this.finish());
          observer.complete();
        });
      });
    };
  }

  fakeDeleteController() {
    observableTimer(100).subscribe(() => {
      this.finish();
      this.ctrlRef.hide();
    });
  }
}

describe('CriticalConfirmationModalComponent', () => {
  let mockComponent: MockComponent;
  let component: CriticalConfirmationModalComponent;
  let mockFixture: ComponentFixture<MockComponent>;
  let fixture: ComponentFixture<CriticalConfirmationModalComponent>;

  configureTestBed({
    declarations: [MockComponent, CriticalConfirmationModalComponent],
    schemas: [NO_ERRORS_SCHEMA],
    imports: [ModalModule.forRoot(), ReactiveFormsModule, MockModule, DirectivesModule],
    providers: [BsModalRef]
  });

  beforeEach(() => {
    mockFixture = TestBed.createComponent(MockComponent);
    mockComponent = mockFixture.componentInstance;
    // Mocking the modals as a lot would be left over
    spyOn(mockComponent.modalService, 'show').and.callFake((modalComp, config) => {
      const ref = new BsModalRef();
      fixture = TestBed.createComponent(CriticalConfirmationModalComponent);
      component = fixture.componentInstance;
      if (config.initialState) {
        component = Object.assign(component, config.initialState);
      }
      fixture.detectChanges();
      ref.content = component;
      return ref;
    });
    mockComponent.openCtrlDriven();
    mockFixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should focus the checkbox form field', () => {
    fixture.detectChanges();
    fixture.whenStable().then(() => {
      const focused = fixture.debugElement.query(By.css(':focus'));
      expect(focused.attributes.id).toBe('confirmation');
      expect(focused.attributes.type).toBe('checkbox');
      const element = document.getElementById('confirmation');
      expect(element === document.activeElement).toBeTruthy();
    });
  });

  it('should throw an error if no action is defined', () => {
    component = Object.assign(component, {
      submitAction: null,
      submitActionObservable: null
    });
    expect(() => component.ngOnInit()).toThrowError('No submit action defined');
  });

  it('should test if the ctrl driven mock is set correctly through mock component', () => {
    expect(component.bodyTemplate).toBeTruthy();
    expect(component.submitAction).toBeTruthy();
    expect(component.submitActionObservable).not.toBeTruthy();
  });

  it('should test if the modal driven mock is set correctly through mock component', () => {
    mockComponent.openModalDriven();
    expect(component.bodyTemplate).toBeTruthy();
    expect(component.submitActionObservable).toBeTruthy();
    expect(component.submitAction).not.toBeTruthy();
  });

  describe('component functions', () => {
    const changeValue = (value) => {
      const ctrl = component.deletionForm.get('confirmation');
      ctrl.setValue(value);
      ctrl.markAsDirty();
      ctrl.updateValueAndValidity();
      fixture.detectChanges();
    };

    it('should test hideModal', () => {
      expect(component.modalRef).toBeTruthy();
      expect(component.hideModal).toBeTruthy();
      spyOn(component.modalRef, 'hide').and.callThrough();
      expect(component.modalRef.hide).not.toHaveBeenCalled();
      component.hideModal();
      expect(component.modalRef.hide).toHaveBeenCalled();
    });

    describe('validate confirmation', () => {
      const testValidation = (submitted: boolean, error: string, expected: boolean) => {
        expect(
          component.deletionForm.showError('confirmation', <NgForm>{ submitted: submitted }, error)
        ).toBe(expected);
      };

      beforeEach(() => {
        component.deletionForm.reset();
      });

      it('should test empty values', () => {
        component.deletionForm.reset();
        testValidation(false, undefined, false);
        testValidation(true, 'required', true);
        component.deletionForm.reset();
        changeValue(true);
        changeValue(false);
        testValidation(true, 'required', true);
      });
    });

    describe('deletion call', () => {
      beforeEach(() => {
        spyOn(component, 'stopLoadingSpinner').and.callThrough();
        spyOn(component, 'hideModal').and.callThrough();
      });

      describe('Controller driven', () => {
        beforeEach(() => {
          spyOn(component, 'submitAction').and.callThrough();
          spyOn(mockComponent.ctrlRef, 'hide').and.callThrough();
        });

        it('should test fake deletion that closes modal', <any>fakeAsync(() => {
          // Before deletionCall
          expect(component.submitAction).not.toHaveBeenCalled();
          // During deletionCall
          component.callSubmitAction();
          expect(component.stopLoadingSpinner).not.toHaveBeenCalled();
          expect(component.hideModal).not.toHaveBeenCalled();
          expect(mockComponent.ctrlRef.hide).not.toHaveBeenCalled();
          expect(component.submitAction).toHaveBeenCalled();
          expect(mockComponent.finished).toBe(undefined);
          // After deletionCall
          tick(2000);
          expect(component.hideModal).not.toHaveBeenCalled();
          expect(mockComponent.ctrlRef.hide).toHaveBeenCalled();
          expect(mockComponent.finished).toEqual([6, 7, 8, 9]);
        }));
      });

      describe('Modal driven', () => {
        beforeEach(() => {
          mockComponent.openModalDriven();
          spyOn(component, 'stopLoadingSpinner').and.callThrough();
          spyOn(component, 'hideModal').and.callThrough();
          spyOn(mockComponent, 'fakeDelete').and.callThrough();
        });

        it('should delete and close modal', <any>fakeAsync(() => {
          // During deletionCall
          component.callSubmitAction();
          expect(mockComponent.finished).toBe(undefined);
          expect(component.hideModal).not.toHaveBeenCalled();
          // After deletionCall
          tick(2000);
          expect(mockComponent.finished).toEqual([6, 7, 8, 9]);
          expect(component.stopLoadingSpinner).not.toHaveBeenCalled();
          expect(component.hideModal).toHaveBeenCalled();
        }));
      });
    });
  });
});

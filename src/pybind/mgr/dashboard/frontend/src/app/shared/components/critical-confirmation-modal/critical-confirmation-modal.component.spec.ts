import { Component, NgModule, NO_ERRORS_SCHEMA, TemplateRef, ViewChild } from '@angular/core';
import { ComponentFixture, fakeAsync, TestBed, tick } from '@angular/core/testing';
import { NgForm, ReactiveFormsModule } from '@angular/forms';

import { NgbActiveModal, NgbModalModule, NgbModalRef } from '@ng-bootstrap/ng-bootstrap';
import { Observable, Subscriber, timer as observableTimer } from 'rxjs';

import { configureTestBed, modalServiceShow } from '../../../../testing/unit-test-helper';
import { DirectivesModule } from '../../directives/directives.module';
import { ModalService } from '../../services/modal.service';
import { AlertPanelComponent } from '../alert-panel/alert-panel.component';
import { LoadingPanelComponent } from '../loading-panel/loading-panel.component';
import { CriticalConfirmationModalComponent } from './critical-confirmation-modal.component';

@NgModule({})
export class MockModule {}

@Component({
  template: `
    <button type="button" class="btn btn-secondary" (click)="openCtrlDriven()">
      <i class="fa fa-times"></i>Deletion Ctrl-Test
      <ng-template #ctrlDescription>
        The spinner is handled by the controller if you have use the modal as ViewChild in order to
        use it's functions to stop the spinner or close the dialog.
      </ng-template>
    </button>

    <button type="button" class="btn btn-secondary" (click)="openModalDriven()">
      <i class="fa fa-times"></i>Deletion Modal-Test
      <ng-template #modalDescription>
        The spinner is handled by the modal if your given deletion function returns a Observable.
      </ng-template>
    </button>
  `
})
class MockComponent {
  @ViewChild('ctrlDescription', { static: true })
  ctrlDescription: TemplateRef<any>;
  @ViewChild('modalDescription', { static: true })
  modalDescription: TemplateRef<any>;
  someData = [1, 2, 3, 4, 5];
  finished: number[];
  ctrlRef: NgbModalRef;
  modalRef: NgbModalRef;

  // Normally private - public was needed for the tests
  constructor(public modalService: ModalService) {}

  openCtrlDriven() {
    this.ctrlRef = this.modalService.show(CriticalConfirmationModalComponent, {
      submitAction: this.fakeDeleteController.bind(this),
      bodyTemplate: this.ctrlDescription
    });
  }

  openModalDriven() {
    this.modalRef = this.modalService.show(CriticalConfirmationModalComponent, {
      submitActionObservable: this.fakeDelete(),
      bodyTemplate: this.modalDescription
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
      this.ctrlRef.close();
    });
  }
}

describe('CriticalConfirmationModalComponent', () => {
  let mockComponent: MockComponent;
  let component: CriticalConfirmationModalComponent;
  let mockFixture: ComponentFixture<MockComponent>;

  configureTestBed(
    {
      declarations: [
        MockComponent,
        CriticalConfirmationModalComponent,
        LoadingPanelComponent,
        AlertPanelComponent
      ],
      schemas: [NO_ERRORS_SCHEMA],
      imports: [ReactiveFormsModule, MockModule, DirectivesModule, NgbModalModule],
      providers: [NgbActiveModal]
    },
    [CriticalConfirmationModalComponent]
  );

  beforeEach(() => {
    mockFixture = TestBed.createComponent(MockComponent);
    mockComponent = mockFixture.componentInstance;
    spyOn(mockComponent.modalService, 'show').and.callFake((_modalComp, config) => {
      const data = modalServiceShow(CriticalConfirmationModalComponent, config);
      component = data.componentInstance;
      return data;
    });
    mockComponent.openCtrlDriven();
    mockFixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
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
    const changeValue = (value: boolean) => {
      const ctrl = component.deletionForm.get('confirmation');
      ctrl.setValue(value);
      ctrl.markAsDirty();
      ctrl.updateValueAndValidity();
      mockFixture.detectChanges();
    };

    it('should test hideModal', () => {
      expect(component.activeModal).toBeTruthy();
      expect(component.hideModal).toBeTruthy();
      spyOn(component.activeModal, 'close').and.callThrough();
      expect(component.activeModal.close).not.toHaveBeenCalled();
      component.hideModal();
      expect(component.activeModal.close).toHaveBeenCalled();
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
          spyOn(mockComponent.ctrlRef, 'close').and.callThrough();
        });

        it('should test fake deletion that closes modal', fakeAsync(() => {
          // Before deletionCall
          expect(component.submitAction).not.toHaveBeenCalled();
          // During deletionCall
          component.callSubmitAction();
          expect(component.stopLoadingSpinner).not.toHaveBeenCalled();
          expect(component.hideModal).not.toHaveBeenCalled();
          expect(mockComponent.ctrlRef.close).not.toHaveBeenCalled();
          expect(component.submitAction).toHaveBeenCalled();
          expect(mockComponent.finished).toBe(undefined);
          // After deletionCall
          tick(2000);
          expect(component.hideModal).not.toHaveBeenCalled();
          expect(mockComponent.ctrlRef.close).toHaveBeenCalled();
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

        it('should delete and close modal', fakeAsync(() => {
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

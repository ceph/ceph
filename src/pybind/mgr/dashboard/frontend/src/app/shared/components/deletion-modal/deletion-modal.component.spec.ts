import { Component, NgModule, NO_ERRORS_SCHEMA, TemplateRef, ViewChild } from '@angular/core';
import { ComponentFixture, fakeAsync, TestBed, tick } from '@angular/core/testing';
import { NgForm, ReactiveFormsModule } from '@angular/forms';
import { By } from '@angular/platform-browser';

import { BsModalRef, BsModalService, ModalModule } from 'ngx-bootstrap';
import { Observable, Subscriber, timer as observableTimer } from 'rxjs';

import { configureTestBed } from '../../../../testing/unit-test-helper';
import { DirectivesModule } from '../../directives/directives.module';
import { DeletionModalComponent } from './deletion-modal.component';

@NgModule({
  entryComponents: [DeletionModalComponent]
})
export class MockModule {}

@Component({
  template: `
    <button type="button"
        class="btn btn-sm btn-primary"
        (click)="openCtrlDriven()">
      <i class="fa fa-fw fa-trash"></i>Deletion Ctrl-Test
      <ng-template #ctrlDescription>
        The spinner is handled by the controller if you have use the modal as ViewChild in order to
        use it's functions to stop the spinner or close the dialog.
      </ng-template>
    </button>

    <button type="button"
            class="btn btn-sm btn-primary"
            (click)="openModalDriven()">
      <i class="fa fa-fw fa-trash"></i>Deletion Modal-Test
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
    this.ctrlRef = this.modalService.show(DeletionModalComponent);
    this.ctrlRef.content.setUp({
      metaType: 'Controller delete handling',
      deletionMethod: this.fakeDeleteController.bind(this),
      description: this.ctrlDescription,
      modalRef: this.ctrlRef
    });
  }

  openModalDriven() {
    this.modalRef = this.modalService.show(DeletionModalComponent);
    this.modalRef.content.setUp({
      metaType: 'Modal delete handling',
      deletionObserver: this.fakeDelete(),
      description: this.modalDescription,
      modalRef: this.modalRef
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

describe('DeletionModalComponent', () => {
  let mockComponent: MockComponent;
  let component: DeletionModalComponent;
  let mockFixture: ComponentFixture<MockComponent>;
  let fixture: ComponentFixture<DeletionModalComponent>;

  configureTestBed({
    declarations: [MockComponent, DeletionModalComponent],
    schemas: [NO_ERRORS_SCHEMA],
    imports: [ModalModule.forRoot(), ReactiveFormsModule, MockModule, DirectivesModule]
  });

  beforeEach(() => {
    mockFixture = TestBed.createComponent(MockComponent);
    mockComponent = mockFixture.componentInstance;
    // Mocking the modals as a lot would be left over
    spyOn(mockComponent.modalService, 'show').and.callFake(() => {
      const ref = new BsModalRef();
      fixture = TestBed.createComponent(DeletionModalComponent);
      component = fixture.componentInstance;
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
    fixture = TestBed.createComponent(DeletionModalComponent);
    fixture.detectChanges();
    fixture.whenStable().then(() => {
      const focused = fixture.debugElement.query(By.css(':focus'));
      expect(focused.attributes.id).toBe('confirmation');
      expect(focused.attributes.type).toBe('checkbox');
      const element = document.getElementById('confirmation');
      expect(element === document.activeElement).toBeTruthy();
    });
  });

  describe('setUp', () => {
    const clearSetup = () => {
      component.metaType = undefined;
      component.deletionObserver = undefined;
      component.description = undefined;
      component.modalRef = undefined;
    };

    const expectSetup = (metaType, observer: boolean, method: boolean, template: boolean) => {
      expect(component.modalRef).toBeTruthy();
      expect(component.metaType).toBe(metaType);
      expect(!!component.deletionObserver).toBe(observer);
      expect(!!component.deletionMethod).toBe(method);
      expect(!!component.description).toBe(template);
    };

    beforeEach(() => {
      clearSetup();
    });

    it('should throw error if no modal reference is given', () => {
      expect(() =>
        component.setUp({
          metaType: undefined,
          modalRef: undefined
        })
      ).toThrowError('No modal reference');
    });

    it('should throw error if no meta type is given', () => {
      expect(() =>
        component.setUp({
          metaType: undefined,
          modalRef: mockComponent.ctrlRef
        })
      ).toThrowError('No meta type');
    });

    it('should throw error if no deletion method is given', () => {
      expect(() =>
        component.setUp({
          metaType: 'Sth',
          modalRef: mockComponent.ctrlRef
        })
      ).toThrowError('No deletion method');
    });

    it('should throw no errors if metaType, modalRef and a deletion method were given', () => {
      component.setUp({
        metaType: 'Observer',
        modalRef: mockComponent.ctrlRef,
        deletionObserver: mockComponent.fakeDelete()
      });
      expectSetup('Observer', true, false, false);
      clearSetup();
      component.setUp({
        metaType: 'Controller',
        modalRef: mockComponent.ctrlRef,
        deletionMethod: mockComponent.fakeDeleteController
      });
      expectSetup('Controller', false, true, false);
    });

    it('should test optional parameters - description', () => {
      component.setUp({
        metaType: 'Description only',
        modalRef: mockComponent.ctrlRef,
        deletionObserver: mockComponent.fakeDelete(),
        description: mockComponent.modalDescription
      });
      expectSetup('Description only', true, false, true);
    });
  });

  it('should test if the ctrl driven mock is set correctly through mock component', () => {
    expect(component.metaType).toBe('Controller delete handling');
    expect(component.description).toBeTruthy();
    expect(component.modalRef).toBeTruthy();
    expect(component.deletionMethod).toBeTruthy();
    expect(component.deletionObserver).not.toBeTruthy();
  });

  it('should test if the modal driven mock is set correctly through mock component', () => {
    mockComponent.openModalDriven();
    expect(component.metaType).toBe('Modal delete handling');
    expect(component.description).toBeTruthy();
    expect(component.modalRef).toBeTruthy();
    expect(component.deletionObserver).toBeTruthy();
    expect(component.deletionMethod).not.toBeTruthy();
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
          spyOn(component, 'deletionMethod').and.callThrough();
          spyOn(mockComponent.ctrlRef, 'hide').and.callThrough();
        });

        it('should test fake deletion that closes modal', <any>fakeAsync(() => {
          // Before deletionCall
          expect(component.deletionMethod).not.toHaveBeenCalled();
          // During deletionCall
          component.deletionCall();
          expect(component.stopLoadingSpinner).not.toHaveBeenCalled();
          expect(component.hideModal).not.toHaveBeenCalled();
          expect(mockComponent.ctrlRef.hide).not.toHaveBeenCalled();
          expect(component.deletionMethod).toHaveBeenCalled();
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
          spyOn(mockComponent.modalRef, 'hide').and.callThrough();
          spyOn(component, 'stopLoadingSpinner').and.callThrough();
          spyOn(component, 'hideModal').and.callThrough();
          spyOn(mockComponent, 'fakeDelete').and.callThrough();
        });

        it('should delete and close modal', <any>fakeAsync(() => {
          // During deletionCall
          component.deletionCall();
          expect(mockComponent.finished).toBe(undefined);
          expect(component.hideModal).not.toHaveBeenCalled();
          expect(mockComponent.modalRef.hide).not.toHaveBeenCalled();
          // After deletionCall
          tick(2000);
          expect(mockComponent.finished).toEqual([6, 7, 8, 9]);
          expect(mockComponent.modalRef.hide).toHaveBeenCalled();
          expect(component.stopLoadingSpinner).not.toHaveBeenCalled();
          expect(component.hideModal).toHaveBeenCalled();
        }));
      });
    });
  });
});

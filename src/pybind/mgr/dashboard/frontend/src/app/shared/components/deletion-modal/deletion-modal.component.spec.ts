import { Component, NgModule, TemplateRef, ViewChild } from '@angular/core';
import { async, ComponentFixture, fakeAsync, TestBed, tick } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';

import { BsModalRef, BsModalService, ModalModule } from 'ngx-bootstrap';
import { Observable } from 'rxjs/Observable';
import { Subscriber } from 'rxjs/Subscriber';

import { ModalComponent } from '../modal/modal.component';
import { SubmitButtonComponent } from '../submit-button/submit-button.component';
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
  @ViewChild('ctrlDescription') ctrlDescription: TemplateRef<any>;
  @ViewChild('modalDescription') modalDescription: TemplateRef<any>;
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
      pattern: 'ctrl-test',
      deletionMethod: this.fakeDeleteController.bind(this),
      description: this.ctrlDescription,
      modalRef: this.ctrlRef
    });
  }

  openModalDriven() {
    this.modalRef = this.modalService.show(DeletionModalComponent);
    this.modalRef.content.setUp({
      metaType: 'Modal delete handling',
      pattern: 'modal-test',
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
      this.ctrlRef.hide();
    });
  }
}

describe('DeletionModalComponent', () => {
  let mockComponent: MockComponent;
  let component: DeletionModalComponent;
  let mockFixture: ComponentFixture<MockComponent>;
  let fixture: ComponentFixture<DeletionModalComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ MockComponent, DeletionModalComponent, ModalComponent,
        SubmitButtonComponent],
      imports: [ModalModule.forRoot(), ReactiveFormsModule, MockModule],
    })
    .compileComponents();
  }));

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

  describe('setUp', () => {
    const clearSetup = () => {
      component.metaType = undefined;
      component.pattern = 'yes';
      component.deletionObserver = undefined;
      component.description = undefined;
      component.modalRef = undefined;
    };

    const expectSetup = (metaType, observer: boolean, method: boolean, pattern,
                         template: boolean) => {
      expect(component.modalRef).toBeTruthy();
      expect(component.metaType).toBe(metaType);
      expect(!!component.deletionObserver).toBe(observer);
      expect(!!component.deletionMethod).toBe(method);
      expect(component.pattern).toBe(pattern);
      expect(!!component.description).toBe(template);
    };

    beforeEach(() => {
      clearSetup();
    });

    it('should throw error if no modal reference is given', () => {
      expect(() => component.setUp({
        metaType: undefined,
        modalRef: undefined
      })).toThrowError('No modal reference');
    });

    it('should throw error if no meta type is given', () => {
      expect(() => component.setUp({
        metaType: undefined,
        modalRef: mockComponent.ctrlRef
      })).toThrowError('No meta type');
    });

    it('should throw error if no deletion method is given', () => {
      expect(() => component.setUp({
        metaType: 'Sth',
        modalRef: mockComponent.ctrlRef
      })).toThrowError('No deletion method');
    });

    it('should throw no errors if metaType, modalRef and a deletion method were given',
        () => {
          component.setUp({
            metaType: 'Observer',
            modalRef: mockComponent.ctrlRef,
            deletionObserver: mockComponent.fakeDelete()
          });
          expectSetup('Observer', true, false, 'yes', false);
          clearSetup();
          component.setUp({
            metaType: 'Controller',
            modalRef: mockComponent.ctrlRef,
            deletionMethod: mockComponent.fakeDeleteController
          });
          expectSetup('Controller', false, true, 'yes', false);
        });

    it('should test optional parameters - pattern and description',
      () => {
        component.setUp({
          metaType: 'Pattern only',
          modalRef: mockComponent.ctrlRef,
          deletionObserver: mockComponent.fakeDelete(),
          pattern: '{sth/!$_8()'
        });
        expectSetup('Pattern only', true, false, '{sth/!$_8()', false);
        clearSetup();
        component.setUp({
          metaType: 'Description only',
          modalRef: mockComponent.ctrlRef,
          deletionObserver: mockComponent.fakeDelete(),
          description: mockComponent.modalDescription
        });
        expectSetup('Description only', true, false, 'yes', true);
        clearSetup();
        component.setUp({
          metaType: 'Description and pattern',
          modalRef: mockComponent.ctrlRef,
          deletionObserver: mockComponent.fakeDelete(),
          description: mockComponent.modalDescription,
          pattern: '{sth/!$_8()'
        });
        expectSetup('Description and pattern', true, false, '{sth/!$_8()', true);
      });
  });

  it('should test if the ctrl driven mock is set correctly through mock component', () => {
    expect(component.metaType).toBe('Controller delete handling');
    expect(component.pattern).toBe('ctrl-test');
    expect(component.description).toBeTruthy();
    expect(component.modalRef).toBeTruthy();
    expect(component.deletionMethod).toBeTruthy();
    expect(component.deletionObserver).not.toBeTruthy();
  });

  it('should test if the modal driven mock is set correctly through mock component', () => {
    mockComponent.openModalDriven();
    expect(component.metaType).toBe('Modal delete handling');
    expect(component.pattern).toBe('modal-test');
    expect(component.description).toBeTruthy();
    expect(component.modalRef).toBeTruthy();
    expect(component.deletionObserver).toBeTruthy();
    expect(component.deletionMethod).not.toBeTruthy();
  });

  describe('component functions', () => {
    const changeValue = (value) => {
      component.confirmation.setValue(value);
      component.confirmation.markAsDirty();
      component.confirmation.updateValueAndValidity();
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

      it('should test regex pattern', () => {
        component.pattern = 'a+b';
        changeValue('ab');
        testInvalidControl(false, 'pattern', true);
        changeValue('a+b');
        testInvalidControl(false, 'pattern', false);
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

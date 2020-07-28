import { Component, NgModule, NO_ERRORS_SCHEMA, TemplateRef, ViewChild } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { By } from '@angular/platform-browser';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbActiveModal, NgbModalModule, NgbModalRef } from '@ng-bootstrap/ng-bootstrap';

import { configureTestBed, FixtureHelper } from '../../../../testing/unit-test-helper';
import { ModalService } from '../../services/modal.service';
import { BackButtonComponent } from '../back-button/back-button.component';
import { ModalComponent } from '../modal/modal.component';
import { SubmitButtonComponent } from '../submit-button/submit-button.component';
import { ConfirmationModalComponent } from './confirmation-modal.component';

@NgModule({})
export class MockModule {}

@Component({
  template: `
    <ng-template #fillTpl>
      Template based description.
    </ng-template>
  `
})
class MockComponent {
  @ViewChild('fillTpl', { static: true })
  fillTpl: TemplateRef<any>;
  modalRef: NgbModalRef;
  returnValue: any;

  // Normally private, but public is needed by tests
  constructor(public modalService: ModalService) {}

  private openModal(extendBaseState = {}) {
    this.modalRef = this.modalService.show(
      ConfirmationModalComponent,
      Object.assign(
        {
          titleText: 'Title is a must have',
          buttonText: 'Action label',
          bodyTpl: this.fillTpl,
          description: 'String based description.',
          onSubmit: () => {
            this.returnValue = 'The submit action has to hide manually.';
          }
        },
        extendBaseState
      )
    );
  }

  basicModal() {
    this.openModal();
  }

  customCancelModal() {
    this.openModal({
      onCancel: () => (this.returnValue = 'If you have todo something besides hiding the modal.')
    });
  }
}

describe('ConfirmationModalComponent', () => {
  let component: ConfirmationModalComponent;
  let fixture: ComponentFixture<ConfirmationModalComponent>;
  let mockComponent: MockComponent;
  let mockFixture: ComponentFixture<MockComponent>;
  let fh: FixtureHelper;

  const expectReturnValue = (v: string) => expect(mockComponent.returnValue).toBe(v);

  configureTestBed({
    declarations: [
      ConfirmationModalComponent,
      BackButtonComponent,
      MockComponent,
      ModalComponent,
      SubmitButtonComponent
    ],
    schemas: [NO_ERRORS_SCHEMA],
    imports: [ReactiveFormsModule, MockModule, RouterTestingModule, NgbModalModule],
    providers: [NgbActiveModal, SubmitButtonComponent]
  });

  beforeEach(() => {
    fh = new FixtureHelper();
    mockFixture = TestBed.createComponent(MockComponent);
    mockComponent = mockFixture.componentInstance;
    mockFixture.detectChanges();

    spyOn(TestBed.inject(ModalService), 'show').and.callFake((_modalComp, config) => {
      fixture = TestBed.createComponent(ConfirmationModalComponent);
      component = fixture.componentInstance;
      component = Object.assign(component, config);
      component.activeModal = { close: () => true } as any;
      spyOn(component.activeModal, 'close').and.callThrough();
      fh.updateFixture(fixture);
    });
  });

  it('should create', () => {
    mockComponent.basicModal();
    expect(component).toBeTruthy();
  });

  describe('Throws errors', () => {
    const expectError = (config: object, expected: string) => {
      mockComponent.basicModal();
      component = Object.assign(component, config);
      expect(() => component.ngOnInit()).toThrowError(expected);
    };

    it('has no submit action defined', () => {
      expectError(
        {
          onSubmit: undefined
        },
        'No submit action defined'
      );
    });

    it('has no title defined', () => {
      expectError(
        {
          titleText: undefined
        },
        'No title defined'
      );
    });

    it('has no action name defined', () => {
      expectError(
        {
          buttonText: undefined
        },
        'No action name defined'
      );
    });

    it('has no description defined', () => {
      expectError(
        {
          bodyTpl: undefined,
          description: undefined
        },
        'No description defined'
      );
    });
  });

  describe('basics', () => {
    beforeEach(() => {
      mockComponent.basicModal();
      spyOn(component, 'onSubmit').and.callThrough();
    });

    it('should show the correct title', () => {
      expect(fh.getText('.modal-title')).toBe('Title is a must have');
    });

    it('should show the correct action name', () => {
      expect(fh.getText('.tc_submitButton')).toBe('Action label');
    });

    it('should use the correct submit action', () => {
      // In order to ignore the `ElementRef` usage of `SubmitButtonComponent`
      spyOn(
        fixture.debugElement.query(By.directive(SubmitButtonComponent)).componentInstance,
        'focusButton'
      );
      fh.clickElement('.tc_submitButton');
      expect(component.onSubmit).toHaveBeenCalledTimes(1);
      expect(component.activeModal.close).toHaveBeenCalledTimes(0);
      expectReturnValue('The submit action has to hide manually.');
    });

    it('should use the default cancel action', () => {
      fh.clickElement('.tc_backButton');
      expect(component.onSubmit).toHaveBeenCalledTimes(0);
      expect(component.activeModal.close).toHaveBeenCalledTimes(1);
      expectReturnValue(undefined);
    });

    it('should show the description', () => {
      expect(fh.getText('.modal-body')).toBe(
        'Template based description.  String based description.'
      );
    });
  });
});

import { Component, NgModule, NO_ERRORS_SCHEMA, TemplateRef, ViewChild } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';

import { BsModalRef, BsModalService, ModalModule } from 'ngx-bootstrap/modal';

import { By } from '@angular/platform-browser';
import {
  configureTestBed,
  FixtureHelper,
  i18nProviders,
  modalServiceShow
} from '../../../../testing/unit-test-helper';
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
  modalRef: BsModalRef;
  returnValue: any;

  // Normally private, but public is needed by tests
  constructor(public modalService: BsModalService) {}

  private openModal(extendBaseState = {}) {
    this.modalRef = this.modalService.show(ConfirmationModalComponent, {
      initialState: Object.assign(
        {
          titleText: 'Title is a must have',
          buttonText: 'Action label',
          bodyTpl: this.fillTpl,
          description: 'String based description.',
          onSubmit: () => {
            this.returnValue = 'The submit action has to hide manually.';
            this.modalRef.hide();
          }
        },
        extendBaseState
      )
    });
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
  let modalService: BsModalService;
  let fh: FixtureHelper;

  /**
   * The hide method of `BsModalService` doesn't emit events during tests that's why it's mocked.
   *
   * The only events of hide are `null`, `'backdrop-click'` and `'esc'` as described here:
   * https://ngx-universal.herokuapp.com/#/modals#service-events
   */
  const hide = (x: string) => modalService.onHide.emit(null || x);

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
    imports: [ModalModule.forRoot(), ReactiveFormsModule, MockModule, RouterTestingModule],
    providers: [BsModalRef, i18nProviders, SubmitButtonComponent]
  });

  beforeEach(() => {
    fh = new FixtureHelper();
    mockFixture = TestBed.createComponent(MockComponent);
    mockComponent = mockFixture.componentInstance;
    mockFixture.detectChanges();
    modalService = TestBed.inject(BsModalService);
    spyOn(modalService, 'show').and.callFake((_modalComp, config) => {
      const data = modalServiceShow(ConfirmationModalComponent, config);
      fixture = data.fixture;
      component = data.component;
      spyOn(component.modalRef, 'hide').and.callFake(hide);
      fh.updateFixture(fixture);
      return data.ref;
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
      spyOn(mockComponent.modalRef, 'hide').and.callFake(hide);
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
      expect(mockComponent.modalRef.hide).toHaveBeenCalledTimes(1);
      expect(component.modalRef.hide).toHaveBeenCalledTimes(0);
      expectReturnValue('The submit action has to hide manually.');
    });

    it('should use the default cancel action', () => {
      fh.clickElement('.tc_backButton');
      expect(mockComponent.modalRef.hide).toHaveBeenCalledTimes(0);
      expect(component.modalRef.hide).toHaveBeenCalledTimes(1);
      expectReturnValue(undefined);
    });

    it('should show the description', () => {
      expect(fh.getText('.modal-body')).toBe(
        'Template based description.  String based description.'
      );
    });
  });

  describe('custom cancel action', () => {
    const expectCancelValue = () =>
      expectReturnValue('If you have todo something besides hiding the modal.');

    beforeEach(() => {
      mockComponent.customCancelModal();
    });

    it('should use custom cancel action', () => {
      fh.clickElement('.tc_backButton');
      expectCancelValue();
    });

    it('should use custom cancel action if escape was pressed', () => {
      hide('esc');
      expectCancelValue();
    });

    it('should use custom cancel action if clicked outside the modal', () => {
      hide('backdrop-click');
      expectCancelValue();
    });

    it('should unsubscribe on destroy', () => {
      hide('backdrop-click');
      expectCancelValue();
      const s = 'This value will not be changed.';
      mockComponent.returnValue = s;
      component.ngOnDestroy();
      hide('backdrop-click');
      expectReturnValue(s);
    });
  });
});

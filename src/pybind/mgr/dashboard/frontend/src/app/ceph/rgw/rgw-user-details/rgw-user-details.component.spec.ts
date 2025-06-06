import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';

import { NgbNavModule } from '@ng-bootstrap/ng-bootstrap';

import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { RgwUserDetailsComponent } from './rgw-user-details.component';
import { ModalService } from 'carbon-components-angular';

describe('RgwUserDetailsComponent', () => {
  let component: RgwUserDetailsComponent;
  let fixture: ComponentFixture<RgwUserDetailsComponent>;
  let modalRef: any;
  configureTestBed({
    declarations: [RgwUserDetailsComponent],
    imports: [BrowserAnimationsModule, HttpClientTestingModule, SharedModule, NgbNavModule],
    provider: [ModalService]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RgwUserDetailsComponent);
    component = fixture.componentInstance;
    component.selection = {};
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should show correct "System" info', () => {
    component.selection = { uid: '', email: '', system: true, keys: [], swift_keys: [] };

    component.ngOnChanges();
    fixture.detectChanges();

    const detailsTab = fixture.debugElement.nativeElement.querySelectorAll(
      '.cds--data-table--sort.cds--data-table--no-border tr td'
    );
    expect(detailsTab[10].textContent).toEqual('System user');
    expect(detailsTab[11].textContent).toEqual('Yes');

    component.selection.system = false;
    component.ngOnChanges();
    fixture.detectChanges();

    expect(detailsTab[11].textContent).toEqual('No');
  });

  it('should show mfa ids only if length > 0', () => {
    component.selection = {
      uid: 'dashboard',
      email: '',
      system: 'true',
      keys: [],
      swift_keys: [],
      mfa_ids: ['testMFA1', 'testMFA2']
    };

    component.ngOnChanges();
    fixture.detectChanges();

    const detailsTab = fixture.debugElement.nativeElement.querySelectorAll(
      '.cds--data-table--sort.cds--data-table--no-border tr td'
    );
    expect(detailsTab[14].textContent).toEqual('MFAs(Id)');
    expect(detailsTab[15].textContent).toEqual('testMFA1, testMFA2');
  });
  it('should test updateKeysSelection', () => {
    component.selection = {
      hasMultiSelection: false,
      hasSelection: false,
      hasSingleSelection: false,
      _selected: []
    };
    component.updateKeysSelection(component.selection);
    expect(component.keysSelection).toEqual(component.selection);
  });
  it('should call showKeyModal when key selection is of type S3', () => {
    component.keysSelection.first = () => {
      return { type: 'S3', ref: { user: '', access_key: '', secret_key: '' } };
    };
    const modalShowSpy = spyOn(component['cdsModalService'], 'show').and.callFake(() => {
      modalRef = {
        setValues: jest.fn(),
        setViewing: jest.fn()
      };
      return modalRef;
    });
    component.showKeyModal();
    expect(modalShowSpy).toHaveBeenCalled();
    // expect(s).toHaveBeenCalledWith( modalRef.componentInstance.setViewing);
  });
  it('should call showKeyModal when key selection is of type Swift', () => {
    component.keysSelection.first = () => {
      return { type: 'Swift', ref: { user: '', access_key: '', secret_key: '' } };
    };
    const modalShowSpy = spyOn(component['cdsModalService'], 'show').and.callFake(() => {
      modalRef = {
        setValues: jest.fn(),
        setViewing: jest.fn()
      };
      return modalRef;
    });
    component.showKeyModal();
    expect(modalShowSpy).toHaveBeenCalled();
    // expect(s).toHaveBeenCalledWith( modalRef.componentInstance.setViewing);
  });
});

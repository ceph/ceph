import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, fakeAsync, TestBed, tick } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { ToastrModule } from 'ngx-toastr';

import { LoadingPanelComponent } from '~/app/shared/components/loading-panel/loading-panel.component';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed, FormHelper } from '~/testing/unit-test-helper';
import { HostFormComponent } from './host-form.component';

describe('HostFormComponent', () => {
  let component: HostFormComponent;
  let fixture: ComponentFixture<HostFormComponent>;
  let formHelper: FormHelper;

  configureTestBed(
    {
      imports: [
        SharedModule,
        HttpClientTestingModule,
        RouterTestingModule,
        ReactiveFormsModule,
        ToastrModule.forRoot()
      ],
      declarations: [HostFormComponent],
      providers: [NgbActiveModal]
    },
    [LoadingPanelComponent]
  );

  beforeEach(() => {
    fixture = TestBed.createComponent(HostFormComponent);
    component = fixture.componentInstance;
    component.ngOnInit();
    formHelper = new FormHelper(component.hostForm);
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should open the form in a modal', () => {
    const nativeEl = fixture.debugElement.nativeElement;
    expect(nativeEl.querySelector('cd-modal')).not.toBe(null);
  });

  it('should validate the network address is valid', fakeAsync(() => {
    formHelper.setValue('addr', '115.42.150.37', true);
    tick();
    formHelper.expectValid('addr');
  }));

  it('should show error if network address is invalid', fakeAsync(() => {
    formHelper.setValue('addr', '666.10.10.20', true);
    tick();
    formHelper.expectError('addr', 'pattern');
  }));

  it('should submit the network address', () => {
    component.hostForm.get('addr').setValue('127.0.0.1');
    fixture.detectChanges();
    component.submit();
    expect(component.addr).toBe('127.0.0.1');
  });

  it('should validate the labels are added', () => {
    const labels = ['label1', 'label2'];
    component.hostForm.get('labels').patchValue(labels);
    fixture.detectChanges();
    component.submit();
    expect(component.allLabels).toBe(labels);
  });

  it('should select maintenance mode', () => {
    component.hostForm.get('maintenance').setValue(true);
    fixture.detectChanges();
    component.submit();
    expect(component.status).toBe('maintenance');
  });
});

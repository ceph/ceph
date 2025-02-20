import { ComponentFixture, TestBed } from '@angular/core/testing';

import { SmbDomainSettingModalComponent } from './smb-domain-setting-modal.component';
import { SharedModule } from '~/app/shared/shared.module';
import { ToastrModule } from 'ngx-toastr';
import { ReactiveFormsModule } from '@angular/forms';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { RouterTestingModule } from '@angular/router/testing';
import { NgbActiveModal, NgbTypeaheadModule } from '@ng-bootstrap/ng-bootstrap';
import { InputModule, ModalModule, SelectModule } from 'carbon-components-angular';
import { CUSTOM_ELEMENTS_SCHEMA, NO_ERRORS_SCHEMA } from '@angular/core';
import { FOO_JOIN_AUTH } from '../smb-join-auth-form/smb-join-auth-form.component.spec';
import { of } from 'rxjs';
import { By } from '@angular/platform-browser';

describe('SmbDomainSettingModalComponent', () => {
  let component: SmbDomainSettingModalComponent;
  let fixture: ComponentFixture<SmbDomainSettingModalComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [SmbDomainSettingModalComponent],
      imports: [
        SharedModule,
        ToastrModule.forRoot(),
        ReactiveFormsModule,
        HttpClientTestingModule,
        RouterTestingModule,
        NgbTypeaheadModule,
        ModalModule,
        InputModule,
        SelectModule
      ],
      schemas: [NO_ERRORS_SCHEMA, CUSTOM_ELEMENTS_SCHEMA],
      providers: [NgbActiveModal, { provide: 'domainSettingsObject', useValue: [[]] }]
    }).compileComponents();

    fixture = TestBed.createComponent(SmbDomainSettingModalComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should add join sources', () => {
    const defaultLength = component.join_sources.length;
    component.addJoinSource();
    expect(component.join_sources.length).toBe(defaultLength + 1);
  });

  it('should call submit', () => {
    component.submit();
    expect(component).toBeTruthy();
  });

  it('should list available join sources', () => {
    component.joinAuths$ = of([FOO_JOIN_AUTH]);
    fixture.whenStable().then(() => {
      const options = fixture.debugElement.queryAll(By.css('select option'));

      expect(options.length).toBe(1);
      expect(options[0].nativeElement.value).toBe('foo');
      expect(options[0].nativeElement.textContent).toBe('foo');
    });
  });
});

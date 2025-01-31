import { ComponentFixture, TestBed } from '@angular/core/testing';

import { SmbDomainSettingModalComponent } from './smb-domain-setting-modal.component';
import { SharedModule } from '~/app/shared/shared.module';
import { ToastrModule } from 'ngx-toastr';
import { ReactiveFormsModule } from '@angular/forms';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { RouterTestingModule } from '@angular/router/testing';
import { NgbActiveModal, NgbTypeaheadModule } from '@ng-bootstrap/ng-bootstrap';
import { InputModule, ModalModule, SelectModule } from 'carbon-components-angular';

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
});

import { ComponentFixture, TestBed } from '@angular/core/testing';

import { SmbDomainSettingModalComponent } from './smb-domain-setting-modal.component';

describe('SmbDomainSettingModalComponent', () => {
  let component: SmbDomainSettingModalComponent;
  let fixture: ComponentFixture<SmbDomainSettingModalComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ SmbDomainSettingModalComponent ]
    })
    .compileComponents();

    fixture = TestBed.createComponent(SmbDomainSettingModalComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});

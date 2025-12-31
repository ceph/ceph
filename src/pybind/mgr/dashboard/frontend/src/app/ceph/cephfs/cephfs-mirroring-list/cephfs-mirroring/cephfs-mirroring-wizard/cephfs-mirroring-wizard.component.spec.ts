import { ComponentFixture, TestBed } from '@angular/core/testing';

import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { SharedModule } from '~/app/shared/shared.module';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { ToastrModule } from 'ngx-toastr';
import { CephfsMirroringWizardComponent } from './cephfs-mirroring-wizard.component';

describe('CephfsMirroringWizardComponent', () => {
  let component: CephfsMirroringWizardComponent;
  let fixture: ComponentFixture<CephfsMirroringWizardComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [
        BrowserAnimationsModule,
        SharedModule,
        HttpClientTestingModule,
        ToastrModule.forRoot(),
        RouterTestingModule
      ],
      declarations: [CephfsMirroringWizardComponent]
    }).compileComponents();

    fixture = TestBed.createComponent(CephfsMirroringWizardComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});

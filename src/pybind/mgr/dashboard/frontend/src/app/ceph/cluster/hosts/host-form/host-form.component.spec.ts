import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';

import { ToastrModule } from 'ngx-toastr';

import { HttpClientTestingModule } from '@angular/common/http/testing';
import { RouterTestingModule } from '@angular/router/testing';
import { configureTestBed, i18nProviders } from '../../../../../testing/unit-test-helper';
import { LoadingPanelComponent } from '../../../../shared/components/loading-panel/loading-panel.component';
import { SharedModule } from '../../../../shared/shared.module';
import { HostFormComponent } from './host-form.component';

describe('HostFormComponent', () => {
  let component: HostFormComponent;
  let fixture: ComponentFixture<HostFormComponent>;

  configureTestBed(
    {
      imports: [
        SharedModule,
        HttpClientTestingModule,
        RouterTestingModule,
        ReactiveFormsModule,
        ToastrModule.forRoot()
      ],
      providers: [i18nProviders],
      declarations: [HostFormComponent]
    },
    [LoadingPanelComponent]
  );

  beforeEach(() => {
    fixture = TestBed.createComponent(HostFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});

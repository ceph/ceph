import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, fakeAsync, TestBed } from '@angular/core/testing';
import { FormBuilder, FormsModule, ReactiveFormsModule } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';

import { ToastrModule } from 'ngx-toastr';

import { of } from 'rxjs';

import { configureTestBed, i18nProviders } from '../../../../testing/unit-test-helper';
import { SharedModule } from '../../../shared/shared.module';
import { SettingFormComponent } from './setting-form.component';

describe('SettingFormComponent', () => {
  let component: SettingFormComponent;
  let fixture: ComponentFixture<SettingFormComponent>;
  const formBuilder: FormBuilder = new FormBuilder();
  let router: Router;

  configureTestBed({
    declarations: [SettingFormComponent],
    imports: [
      FormsModule,
      ReactiveFormsModule,
      SharedModule,
      HttpClientTestingModule,
      RouterTestingModule,
      ToastrModule.forRoot()
    ],
    providers: [
      i18nProviders,
      { provide: FormBuilder, useValue: formBuilder },
      { provide: ActivatedRoute, useValue: { params: of({ name: 'grafana' }) } }
    ]
  });

  const setUpSettingComponent = () => {
    fixture = TestBed.createComponent(SettingFormComponent);
    component = fixture.componentInstance;
  };

  beforeEach(() => {
    setUpSettingComponent();
    router = TestBed.get(Router);
    spyOn(router, 'navigate').and.stub();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('If the setting is Grafana', () => {
    const setUrl = (url) => {
      Object.defineProperty(router, 'url', { value: url });
      setUpSettingComponent(); // Renew of component needed because the constructor has to be called
    };

    it('is in edit mode for setting', () => {
      setUrl('/settings/edit/grafana');
    });

    it('Configure other value of form', fakeAsync(() => {
      expect(component.settingName).toBe('grafana');
    }));
  });
});

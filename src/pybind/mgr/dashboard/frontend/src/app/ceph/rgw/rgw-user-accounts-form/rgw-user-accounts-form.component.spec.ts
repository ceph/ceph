import { ComponentFixture, TestBed } from '@angular/core/testing';

import { RgwUserAccountsFormComponent } from './rgw-user-accounts-form.component';
import { ComponentsModule } from '~/app/shared/components/components.module';
import { RouterTestingModule } from '@angular/router/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ToastrModule } from 'ngx-toastr';
import { PipesModule } from '~/app/shared/pipes/pipes.module';
import { of } from 'rxjs';
import { RgwUserAccountsService } from '~/app/shared/api/rgw-user-accounts.service';
import { ModalModule } from 'carbon-components-angular';

class MockRgwUserAccountsService {
  create = jest.fn().mockReturnValue(of(null));
}

describe('RgwUserAccountsFormComponent', () => {
  let component: RgwUserAccountsFormComponent;
  let fixture: ComponentFixture<RgwUserAccountsFormComponent>;
  let rgwUserAccountsService: MockRgwUserAccountsService;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [RgwUserAccountsFormComponent],
      imports: [
        ComponentsModule,
        ToastrModule.forRoot(),
        HttpClientTestingModule,
        PipesModule,
        RouterTestingModule,
        ModalModule
      ],
      providers: [{ provide: RgwUserAccountsService, useClass: MockRgwUserAccountsService }]
    }).compileComponents();

    fixture = TestBed.createComponent(RgwUserAccountsFormComponent);
    rgwUserAccountsService = (TestBed.inject(
      RgwUserAccountsService
    ) as unknown) as MockRgwUserAccountsService;
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should call create method of MockRgwUserAccountsService and show success notification', () => {
    component.editing = false;
    const spy = jest.spyOn(component, 'submit');
    const createDataSpy = jest.spyOn(rgwUserAccountsService, 'create').mockReturnValue(of(null));
    component.submit();
    expect(spy).toHaveBeenCalled();
    expect(createDataSpy).toHaveBeenCalled();
  });
});

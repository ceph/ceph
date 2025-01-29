import { ComponentFixture, TestBed } from '@angular/core/testing';

import { RgwUserAccountsComponent } from './rgw-user-accounts.component';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ToastrModule } from 'ngx-toastr';
import { PipesModule } from '~/app/shared/pipes/pipes.module';
import { RouterTestingModule } from '@angular/router/testing';
import { ComponentsModule } from '~/app/shared/components/components.module';

describe('RgwUserAccountsComponent', () => {
  let component: RgwUserAccountsComponent;
  let fixture: ComponentFixture<RgwUserAccountsComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [RgwUserAccountsComponent],
      imports: [
        ComponentsModule,
        ToastrModule.forRoot(),
        HttpClientTestingModule,
        PipesModule,
        RouterTestingModule
      ]
    }).compileComponents();

    fixture = TestBed.createComponent(RgwUserAccountsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});

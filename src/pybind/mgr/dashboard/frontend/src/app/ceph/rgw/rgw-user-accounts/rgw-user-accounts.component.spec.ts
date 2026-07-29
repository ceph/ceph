import { configureTestBed } from '~/testing/unit-test-helper';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { RgwUserAccountsComponent } from './rgw-user-accounts.component';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { PipesModule } from '~/app/shared/pipes/pipes.module';
import { RouterTestingModule } from '@angular/router/testing';
import { ComponentsModule } from '~/app/shared/components/components.module';

describe('RgwUserAccountsComponent', () => {
  let component: RgwUserAccountsComponent;
  let fixture: ComponentFixture<RgwUserAccountsComponent>;

  configureTestBed({
    declarations: [RgwUserAccountsComponent],
    imports: [ComponentsModule, HttpClientTestingModule, PipesModule, RouterTestingModule]
  });

  beforeEach(async () => {
    fixture = TestBed.createComponent(RgwUserAccountsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});

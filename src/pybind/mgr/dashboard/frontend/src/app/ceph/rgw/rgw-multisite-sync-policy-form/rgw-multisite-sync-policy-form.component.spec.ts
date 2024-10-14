import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RgwMultisiteSyncPolicyFormComponent } from './rgw-multisite-sync-policy-form.component';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ToastrModule } from 'ngx-toastr';
import { ReactiveFormsModule } from '@angular/forms';
import { PipesModule } from '~/app/shared/pipes/pipes.module';
import { ComponentsModule } from '~/app/shared/components/components.module';
import { RouterTestingModule } from '@angular/router/testing';

describe('RgwMultisiteSyncPolicyFormComponent', () => {
  let component: RgwMultisiteSyncPolicyFormComponent;
  let fixture: ComponentFixture<RgwMultisiteSyncPolicyFormComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [RgwMultisiteSyncPolicyFormComponent],
      imports: [
        HttpClientTestingModule,
        ReactiveFormsModule,
        ToastrModule.forRoot(),
        PipesModule,
        ComponentsModule,
        RouterTestingModule
      ],
      providers: []
    }).compileComponents();

    fixture = TestBed.createComponent(RgwMultisiteSyncPolicyFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});

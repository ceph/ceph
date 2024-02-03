import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { ToastrModule } from 'ngx-toastr';
import { SharedModule } from '~/app/shared/shared.module';

import { CreateRgwServiceEntitiesComponent } from './create-rgw-service-entities.component';
import { configureTestBed } from '~/testing/unit-test-helper';

describe('CreateRgwServiceEntitiesComponent', () => {
  let component: CreateRgwServiceEntitiesComponent;
  let fixture: ComponentFixture<CreateRgwServiceEntitiesComponent>;

  configureTestBed({
    imports: [
      SharedModule,
      ReactiveFormsModule,
      RouterTestingModule,
      HttpClientTestingModule,
      ToastrModule.forRoot()
    ],
    providers: [NgbActiveModal],
    declarations: [CreateRgwServiceEntitiesComponent]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(CreateRgwServiceEntitiesComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});

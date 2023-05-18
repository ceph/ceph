import { ComponentFixture, TestBed } from '@angular/core/testing';

import { CreateRgwServiceEntitiesComponent } from './create-rgw-service-entities.component';

describe('CreateRgwServiceEntitiesComponent', () => {
  let component: CreateRgwServiceEntitiesComponent;
  let fixture: ComponentFixture<CreateRgwServiceEntitiesComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [CreateRgwServiceEntitiesComponent]
    }).compileComponents();
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

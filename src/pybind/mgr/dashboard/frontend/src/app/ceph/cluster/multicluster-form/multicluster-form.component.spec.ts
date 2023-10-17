import { ComponentFixture, TestBed } from '@angular/core/testing';

import { MulticlusterFormComponent } from './multicluster-form.component';

describe('MulticlusterFormComponent', () => {
  let component: MulticlusterFormComponent;
  let fixture: ComponentFixture<MulticlusterFormComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ MulticlusterFormComponent ]
    })
    .compileComponents();

    fixture = TestBed.createComponent(MulticlusterFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});

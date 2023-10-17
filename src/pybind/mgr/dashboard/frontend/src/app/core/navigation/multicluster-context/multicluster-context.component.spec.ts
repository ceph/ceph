import { ComponentFixture, TestBed } from '@angular/core/testing';

import { MulticlusterContextComponent } from './multicluster-context.component';

describe('MulticlusterContextComponent', () => {
  let component: MulticlusterContextComponent;
  let fixture: ComponentFixture<MulticlusterContextComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ MulticlusterContextComponent ]
    })
    .compileComponents();

    fixture = TestBed.createComponent(MulticlusterContextComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});

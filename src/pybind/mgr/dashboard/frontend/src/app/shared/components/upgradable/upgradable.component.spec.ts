import { ComponentFixture, TestBed } from '@angular/core/testing';

import { UpgradableComponent } from './upgradable.component';
import { HttpClientTestingModule } from '@angular/common/http/testing';

describe('UpgradableComponent', () => {
  let component: UpgradableComponent;
  let fixture: ComponentFixture<UpgradableComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [UpgradableComponent],
      imports: [HttpClientTestingModule]
    }).compileComponents();

    fixture = TestBed.createComponent(UpgradableComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});

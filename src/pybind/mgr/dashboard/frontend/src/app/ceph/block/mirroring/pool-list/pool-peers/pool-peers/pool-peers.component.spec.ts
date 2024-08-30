import { ComponentFixture, TestBed } from '@angular/core/testing';

import { PoolPeersComponent } from './pool-peers.component';

describe('PoolPeersComponent', () => {
  let component: PoolPeersComponent;
  let fixture: ComponentFixture<PoolPeersComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [PoolPeersComponent]
    }).compileComponents();

    fixture = TestBed.createComponent(PoolPeersComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});

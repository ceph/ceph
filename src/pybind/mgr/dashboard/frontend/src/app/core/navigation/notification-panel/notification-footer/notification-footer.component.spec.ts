import { ComponentFixture, TestBed } from '@angular/core/testing';
import { NotificationFooterComponent } from './notification-footer.component';

describe('NotificationFooterComponent', () => {
  let component: NotificationFooterComponent;
  let fixture: ComponentFixture<NotificationFooterComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [NotificationFooterComponent]
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(NotificationFooterComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should render view all button', () => {
    const compiled = fixture.nativeElement as HTMLElement;
    const button = compiled.querySelector('cds-button');
    expect(button?.textContent).toContain('View all');
  });
});

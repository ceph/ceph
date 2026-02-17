import { ComponentFixture, TestBed } from '@angular/core/testing';
import { CephfsGenerateTokenComponent } from './cephfs-generate-token.component';

describe('CephfsGenerateTokenComponent', () => {
  let component: CephfsGenerateTokenComponent;
  let fixture: ComponentFixture<CephfsGenerateTokenComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [ReactiveFormsModule, RadioModule],
      declarations: [CephfsMirroringWizardComponent],
      providers: [
        FormBuilder,
        { provide: WizardStepsService, useValue: wizardStepsService },
        { provide: Router, useValue: router }
      ],
      schemas: [NO_ERRORS_SCHEMA]
    }).compileComponents();

    fixture = TestBed.createComponent(CephfsGenerateTokenComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create the component', () => {
    expect(component).toBeTruthy();
  });
});

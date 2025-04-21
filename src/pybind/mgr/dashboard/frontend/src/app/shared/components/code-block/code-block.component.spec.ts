import { ComponentFixture, TestBed } from '@angular/core/testing';

import { CodeBlockComponent } from './code-block.component';
import { configureTestBed } from '~/testing/unit-test-helper';

describe('CodeBlockComponent', () => {
  let component: CodeBlockComponent;
  let fixture: ComponentFixture<CodeBlockComponent>;

  configureTestBed({
    declarations: [CodeBlockComponent]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(CodeBlockComponent);
    component = fixture.componentInstance;
    component.codes = [];
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should show single codeblock if there are only one code', () => {
    component.codes = ['code'];
    fixture.detectChanges();
    expect(fixture.nativeElement.querySelector('#singleCodeBlock')).not.toBeNull();
    expect(fixture.nativeElement.querySelector('#bigCodeBlock')).toBeNull();
  });

  it('should show single codeblock if there are only one code', () => {
    component.codes = ['code1', 'code2'];
    fixture.detectChanges();
    expect(fixture.nativeElement.querySelector('#bigCodeBlock')).not.toBeNull();
    expect(fixture.nativeElement.querySelector('#singleCodeBlock')).toBeNull();
  });
});

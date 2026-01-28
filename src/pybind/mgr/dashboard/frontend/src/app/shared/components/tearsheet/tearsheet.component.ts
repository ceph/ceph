import {
  ChangeDetectorRef,
  Component,
  ContentChildren,
  EventEmitter,
  Input,
  OnInit,
  Output,
  QueryList,
  AfterViewChecked
} from '@angular/core';
import { FormBuilder } from '@angular/forms';
import { Step } from 'carbon-components-angular';
import { TearsheetStepComponent } from '../tearsheet-step/tearsheet-step.component';
import { ModalCdsService } from '../../services/modal-cds.service';
import { ActivatedRoute } from '@angular/router';
import { Location } from '@angular/common';
import { ConfirmationModalComponent } from '../confirmation-modal/confirmation-modal.component';

@Component({
  selector: 'cd-tearsheet',
  standalone: false,
  templateUrl: './tearsheet.component.html',
  styleUrls: ['./tearsheet.component.scss']
})
export class TearsheetComponent implements OnInit, AfterViewChecked {
  @Input() title!: string;
  @Input() steps!: Array<Step>;
  @Input() description!: string;
  @Input() submitButtonLabel: string = $localize`Create`;
  @Input() type: 'full' | 'wide' = 'wide';

  @Output() submitRequested = new EventEmitter<void>();
  @Output() closeRequested = new EventEmitter<void>();

  @ContentChildren(TearsheetStepComponent)
  stepContents!: QueryList<TearsheetStepComponent>;

  get activeStepTemplate() {
    return this.stepContents?.toArray()[this.currentStep]?.template;
  }

  currentStep: number = 0;
  lastStep: number = null;
  isOpen: boolean = true;
  hasModalOutlet: boolean = false;

  constructor(
    protected formBuilder: FormBuilder,
    private changeDetectorRef: ChangeDetectorRef,
    private cdsModalService: ModalCdsService,
    private route: ActivatedRoute,
    private location: Location
  ) {}

  ngOnInit() {
    this.lastStep = this.steps.length - 1;
    this.hasModalOutlet = this.route.outlet === 'modal';
  }

  onStepSelect(event: { step: Step; index: number }) {
    this.currentStep = event.index;
  }

  closeTearsheet() {
    if (this.type === 'full') {
      this.closeFullTearsheet();
    } else {
      this.closeWideTearsheet();
    }
  }

  closeWideTearsheet() {
    this.closeRequested.emit();
    this.isOpen = false;
    if (this.hasModalOutlet) {
      this.location.back();
    } else {
      this.cdsModalService.dismissAll();
    }
  }

  onPrevious() {
    if (this.currentStep !== 0) {
      this.currentStep = this.currentStep - 1;
    }
  }

  onNext() {
    if (this.currentStep !== this.lastStep && !this.steps[this.currentStep].invalid) {
      this.currentStep = this.currentStep + 1;
    }
  }

  onSubmit() {
    if (!this.steps[this.currentStep].invalid) {
      this.submitRequested.emit();
    }
  }

  closeFullTearsheet() {
    this.cdsModalService.show(ConfirmationModalComponent, {
      titleText: $localize`Are you sure you want to cancel ?`,
      description: $localize`If you cancel, the information you have entered won't be saved.`,
      buttonText: $localize`Cancel`,
      cancelText: $localize`Return to form`,
      onSubmit: () => {
        this.isOpen = false;
        this.cdsModalService.dismissAll();
        this.location.back();
      },
      submitBtnType: 'danger',
      showCancel: true
    });
  }

  ngAfterViewChecked() {
    this.changeDetectorRef.detectChanges();
  }
}

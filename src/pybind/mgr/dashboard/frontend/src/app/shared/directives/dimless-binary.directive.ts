import {
  Directive,
  ElementRef,
  EventEmitter,
  HostListener,
  Input,
  OnInit,
  Output
} from '@angular/core';
import { NgControl } from '@angular/forms';

import * as _ from 'lodash';

import { DimlessBinaryPipe } from '../pipes/dimless-binary.pipe';
import { FormatterService } from '../services/formatter.service';

@Directive({
  selector: '[cdDimlessBinary]'
})
export class DimlessBinaryDirective implements OnInit {

  @Output() ngModelChange: EventEmitter<any> = new EventEmitter();
  @Input() minBytes: number;
  @Input() maxBytes: number;
  @Input() roundPower: number;
  @Input() defaultUnit: string;

  private el: HTMLInputElement;

  constructor(private elementRef: ElementRef,
              private control: NgControl,
              private dimlessBinaryPipe: DimlessBinaryPipe,
              private formatter: FormatterService) {
    this.el = this.elementRef.nativeElement;
  }

  ngOnInit() {
    this.setValue(this.el.value);
  }

  setValue(value) {
    const size = this.formatter.parseFloat(value, 'b', this.defaultUnit || 'm');
    const roundedSize = this.round(size);
    this.el.value = this.dimlessBinaryPipe.transform(roundedSize);
    if (size !== null) {
      this.ngModelChange.emit(this.el.value);
      this.control.control.setValue(this.el.value);
    } else {
      this.ngModelChange.emit(null);
      this.control.control.setValue(null);
    }
  }

  round(size) {
    if (size !== null && size !== 0) {
      if (!_.isUndefined(this.minBytes) && size < this.minBytes) {
        return this.minBytes;
      }
      if (!_.isUndefined(this.maxBytes) && size > this.maxBytes) {
        return this.maxBytes;
      }
      if (!_.isUndefined(this.roundPower)) {
        const power = Math.round(Math.log(size) / Math.log(this.roundPower));
        return Math.pow(this.roundPower, power);
      }
    }
    return size;
  }

  @HostListener('blur', ['$event.target.value'])
  onBlur(value) {
    this.setValue(value);
  }

}

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

import _ from 'lodash';

import { DimlessBinaryPerSecondPipe } from '../pipes/dimless-binary-per-second.pipe';
import { FormatterService } from '../services/formatter.service';

@Directive({
  selector: '[cdDimlessBinaryPerSecond]'
})
export class DimlessBinaryPerSecondDirective implements OnInit {
  @Output()
  ngModelChange: EventEmitter<any> = new EventEmitter();

  /**
   * Event emitter for letting this directive know that the data has (asynchronously) been loaded
   * and the value needs to be adapted by this directive.
   */
  @Input()
  ngDataReady: EventEmitter<any>;

  /**
   * Minimum size in bytes.
   * If user enter a value lower than <minBytes>,
   * the model will automatically be update to <minBytes>.
   *
   * If <roundPower> is used, this value should be a power of <roundPower>.
   *
   * Example:
   *   Given minBytes=4096 (4KiB), if user type 1KiB, then model will be updated to 4KiB
   */
  @Input()
  minBytes: number;

  /**
   * Maximum size in bytes.
   * If user enter a value greater than <maxBytes>,
   * the model will automatically be update to <maxBytes>.
   *
   * If <roundPower> is used, this value should be a power of <roundPower>.
   *
   * Example:
   *   Given maxBytes=3145728 (3MiB), if user type 4MiB, then model will be updated to 3MiB
   */
  @Input()
  maxBytes: number;

  /**
   * Value will be rounded up the nearest power of <roundPower>
   *
   * Example:
   *   Given roundPower=2, if user type 7KiB, then model will be updated to 8KiB
   *   Given roundPower=2, if user type 5KiB, then model will be updated to 4KiB
   */
  @Input()
  roundPower: number;

  /**
   * Default unit that should be used when user do not type a unit.
   * By default, "MiB" will be used.
   *
   * Example:
   *   Given defaultUnit=null, if user type 7, then model will be updated to 7MiB
   *   Given defaultUnit=k, if user type 7, then model will be updated to 7KiB
   */
  @Input()
  defaultUnit: string;

  private el: HTMLInputElement;

  constructor(
    private elementRef: ElementRef,
    private control: NgControl,
    private dimlessBinaryPerSecondPipe: DimlessBinaryPerSecondPipe,
    private formatter: FormatterService
  ) {
    this.el = this.elementRef.nativeElement;
  }

  ngOnInit() {
    this.setValue(this.el.value);
    if (this.ngDataReady) {
      this.ngDataReady.subscribe(() => this.setValue(this.el.value));
    }
  }

  setValue(value: string) {
    if (/^[\d.]+$/.test(value)) {
      value += this.defaultUnit || 'm';
    }
    const size = this.formatter.toBytes(value, 0);
    const roundedSize = this.round(size);
    this.el.value = this.dimlessBinaryPerSecondPipe.transform(roundedSize);
    if (size !== null) {
      this.ngModelChange.emit(this.el.value);
      this.control.control.setValue(this.el.value);
    } else {
      this.ngModelChange.emit(null);
      this.control.control.setValue(null);
    }
  }

  round(size: number) {
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
  onBlur(value: string) {
    this.setValue(value);
  }
}

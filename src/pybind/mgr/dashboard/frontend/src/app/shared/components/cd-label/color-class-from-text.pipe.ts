import { Pipe, PipeTransform } from '@angular/core';

@Pipe({
  name: 'colorClassFromText'
})
export class ColorClassFromTextPipe implements PipeTransform {
  readonly cssClasses: string[] = [
    'badge-cd-label-green',
    'badge-cd-label-cyan',
    'badge-cd-label-purple',
    'badge-cd-label-light-blue',
    'badge-cd-label-gold',
    'badge-cd-label-light-green'
  ];

  transform(text: string): string {
    let hash = 0;
    let charCode = 0;
    if (text) {
      for (let i = 0; i < text.length; i++) {
        charCode = text.charCodeAt(i);
        // tslint:disable-next-line:no-bitwise
        hash = Math.abs((hash << 5) - hash + charCode);
      }
    }
    return this.cssClasses[hash % this.cssClasses.length];
  }
}

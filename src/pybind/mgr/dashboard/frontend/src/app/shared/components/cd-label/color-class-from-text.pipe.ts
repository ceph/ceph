import { Pipe, PipeTransform } from '@angular/core';

@Pipe({
  name: 'colorClassFromText'
})
export class ColorClassFromTextPipe implements PipeTransform {
  readonly cssClasses: string[] = [
    'tags-cd-label-green',
    'tags-cd-label-cyan',
    'tags-cd-label-purple',
    'tags-cd-label-light-blue',
    'tags-cd-label-gold',
    'tags-cd-label-light-green'
  ];

  transform(text: string): string {
    let hash = 0;
    let charCode = 0;
    if (text) {
      for (let i = 0; i < text.length; i++) {
        charCode = text.charCodeAt(i);
        // eslint-disable-next-line no-bitwise
        hash = Math.abs((hash << 5) - hash + charCode);
      }
    }
    return this.cssClasses[hash % this.cssClasses.length];
  }
}

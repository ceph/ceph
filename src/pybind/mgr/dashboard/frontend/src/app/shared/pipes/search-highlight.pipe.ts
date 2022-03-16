import { Pipe, PipeTransform } from '@angular/core';

@Pipe({
  name: 'searchHighlight'
})
export class SearchHighlightPipe implements PipeTransform {
  transform(value: string, args: string): string {
    if (!args) {
      return value;
    }
    args = this.escapeRegExp(args);
    const regex = new RegExp(args, 'gi');
    const match = value.match(regex);

    if (!match) {
      return value;
    }

    return value.replace(regex, '<mark>$&</mark>');
  }

  private escapeRegExp(str: string) {
    // $& means the whole matched string
    return str.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  }
}

import { Pipe, PipeTransform } from '@angular/core';

@Pipe({
  name: 'path'
})
export class PathPipe implements PipeTransform {
  transform(value: unknown): string {
    if (!value) return '';
    const splittedPath = value.toString().split('/');

    if (splittedPath[0] === '') {
      splittedPath.shift();
      return `/${splittedPath[0]}/.../${splittedPath[splittedPath.length - 1]}`;
    }
    return `${splittedPath[0]}/.../${splittedPath[splittedPath.length - 1]}`;
  }
}

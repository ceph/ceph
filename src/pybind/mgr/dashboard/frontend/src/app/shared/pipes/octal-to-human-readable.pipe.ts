import { Pipe, PipeTransform } from '@angular/core';

@Pipe({
  name: 'octalToHumanReadable'
})
export class OctalToHumanReadablePipe implements PipeTransform {
  transform(value: number, toTableArray = false): any {
    if (!value) {
      return [];
    }
    const permissionSummary = [];
    const permissions = ['---', '--x', '-w-', '-wx', 'r--', 'r-x', 'rw-', 'rwx'];
    const octal = value.toString(8).padStart(7, '0');
    const digits = octal.split('');

    const fileType = this.getFileTypeSymbol(parseInt(digits[1] + digits[2]));
    const owner = permissions[parseInt(digits[4])];
    const group = permissions[parseInt(digits[5])];
    const others = permissions[parseInt(digits[6])];

    if (toTableArray) {
      return {
        owner: this.getItem(owner),
        group: this.getItem(group),
        others: this.getItem(others)
      };
    }

    if (fileType !== 'directory') {
      permissionSummary.push({
        content: fileType,
        class: 'badge-primary me-1'
      });
    }

    if (owner !== '---') {
      permissionSummary.push({
        content: `owner: ${owner}`,
        class: 'badge-primary me-1'
      });
    }

    if (group !== '---') {
      permissionSummary.push({
        content: `group: ${group}`,
        class: 'badge-primary me-1'
      });
    }

    if (others !== '---') {
      permissionSummary.push({
        content: `others: ${others}`,
        class: 'badge-primary me-1'
      });
    }

    if (permissionSummary.length === 0) {
      return [
        {
          content: 'no permissions',
          class: 'badge-warning me-1',
          toolTip: `owner: ${owner}, group: ${group}, others: ${others}`
        }
      ];
    }

    return permissionSummary;
  }

  private getFileTypeSymbol(fileType: number): string {
    switch (fileType) {
      case 1:
        return 'fifo';
      case 2:
        return 'character';
      case 4:
        return 'directory';
      case 6:
        return 'block';
      case 10:
        return 'regular';
      case 12:
        return 'symbolic-link';
      default:
        return '-';
    }
  }

  private getItem(item: string) {
    const returnlist = [];
    if (item.includes('r')) returnlist.push('read');
    if (item.includes('w')) returnlist.push('write');
    if (item.includes('x')) returnlist.push('execute');
    return returnlist;
  }
}

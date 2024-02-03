import { Pipe, PipeTransform } from '@angular/core';

@Pipe({
  name: 'duration',
  pure: false
})
export class DurationPipe implements PipeTransform {
  /**
   * Translates seconds into human readable format of seconds, minutes, hours, days, and years
   * source: https://stackoverflow.com/a/34270811
   *
   * @param  {number} seconds The number of seconds to be processed
   * @return {string}         The phrase describing the amount of time
   */
  transform(seconds: number): string {
    if (seconds === null || seconds <= 0) {
      return '';
    }
    const levels = [
      [`${Math.floor(seconds / 31536000)}`, 'years'],
      [`${Math.floor((seconds % 31536000) / 86400)}`, 'days'],
      [`${Math.floor((seconds % 86400) / 3600)}`, 'hours'],
      [`${Math.floor((seconds % 3600) / 60)}`, 'minutes'],
      [`${Math.floor(seconds % 60)}`, 'seconds']
    ];
    let returntext = '';

    for (let i = 0, max = levels.length; i < max; i++) {
      if (levels[i][0] === '0') {
        continue;
      }
      returntext +=
        ' ' +
        levels[i][0] +
        ' ' +
        (levels[i][0] === '1' ? levels[i][1].substr(0, levels[i][1].length - 1) : levels[i][1]);
    }
    return returntext.trim() || '1 second';
  }
}

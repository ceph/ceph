import { Pipe, PipeTransform } from '@angular/core';

@Pipe({
  name: 'mdsSummary'
})
export class MdsSummaryPipe implements PipeTransform {
  transform(value: any): any {
    if (!value) {
      return null;
    }

    let standbyReplay = 0;
    let activeCount = 0;
    value?.mdsmap?.forEach((mds: any) => {
      if (mds.state === 'up:standby-replay') {
        standbyReplay += 1;
      } else {
        activeCount += 1;
      }
    });

    const standbyCount = (value.standby || 0) + standbyReplay;
    const totalCount = activeCount + standbyCount;
    const mdsSummary = {
      success: activeCount,
      info: standbyCount,
      total: totalCount
    };

    return mdsSummary;
  }
}

import { Pipe, PipeTransform } from '@angular/core';
import { MultiCluster } from '../models/multi-cluster';

@Pipe({
  name: 'shortenName'
})
export class ShortenNamePipe implements PipeTransform {
  transform(cluster: MultiCluster, shortenFsid: boolean = false): string {
    const name =
      shortenFsid && cluster?.name?.length > 5
        ? `${cluster.name.substring(0, 5)}...`
        : cluster?.name;
    return `${cluster?.cluster_alias} (${name})`;
  }
}

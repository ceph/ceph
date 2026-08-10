import { Component, OnInit, OnDestroy } from '@angular/core';
import { forkJoin, Subscription } from 'rxjs';
import { PoolService } from '~/app/shared/api/pool.service';
import { RbdService } from '~/app/shared/api/rbd.service';

@Component({
  selector: 'cd-rbd-performance',
  templateUrl: './rbd-performance.component.html',
  styleUrls: ['./rbd-performance.component.scss'],
  standalone: false
})
export class RbdPerformanceComponent implements OnInit, OnDestroy {
  loading = true;
  totalPools = 0;
  totalImages = 0;
  totalNamespaces = 0;
  totalTrashImages = 0;

  private subscription = new Subscription();

  constructor(
    private rbdService: RbdService,
    private poolService: PoolService
  ) {}

  ngOnInit() {
    this.refresh();
  }

  refresh() {
    this.loading = true;
    
    const rbdList$ = this.rbdService.list({});
    const trashList$ = this.rbdService.listTrash();
    const pools$ = this.poolService.list(['pool_name', 'type', 'application_metadata']);

    this.subscription.add(
      forkJoin([rbdList$, trashList$, pools$]).subscribe(
        ([rbdPools, trashImages, allPools]: [any[], any[], any[]]) => {
          // Calculate pools and images count
          this.totalPools = rbdPools.length;
          this.totalImages = rbdPools.reduce((acc, pool) => acc + (pool.value?.length || 0), 0);
          
          // Calculate trash images count
          this.totalTrashImages = trashImages.reduce((acc, pool) => acc + (pool.value?.length || 0), 0);

          // Fetch namespaces for replicated RBD pools
          const rbdReplicatedPools = allPools.filter(
            (pool: any) => this.rbdService.isRBDPool(pool) && pool.type === 'replicated'
          );

          if (rbdReplicatedPools.length > 0) {
            const nsQueries = rbdReplicatedPools.map((pool) =>
              this.rbdService.listNamespaces(pool.pool_name)
            );
            this.subscription.add(
              forkJoin(nsQueries).subscribe(
                (namespacesList: any[][]) => {
                  this.totalNamespaces = namespacesList.reduce((acc, ns) => acc + (ns?.length || 0), 0);
                  this.loading = false;
                },
                () => {
                  this.loading = false;
                }
              )
            );
          } else {
            this.totalNamespaces = 0;
            this.loading = false;
          }
        },
        () => {
          this.loading = false;
        }
      )
    );
  }

  ngOnDestroy() {
    this.subscription.unsubscribe();
  }
}

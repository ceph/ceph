import { Component, OnDestroy, OnInit } from '@angular/core';
import { ActivatedRoute, NavigationEnd, ParamMap, Router } from '@angular/router';
import { Subscription } from 'rxjs';
import { filter } from 'rxjs/operators';

enum TABS {
  overview = 'overview',
  mirrorPaths = 'mirror-paths'
}

@Component({
  selector: 'cd-cephfs-mirroring-fs-tabs',
  templateUrl: './cephfs-mirroring-fs-tabs.component.html',
  styleUrls: ['./cephfs-mirroring-fs-tabs.component.scss'],
  standalone: false
})
export class CephfsMirroringFsTabsComponent implements OnInit, OnDestroy {
  fsName = '';
  displayFsName = '';
  activeTab: TABS = TABS.overview;

  private subs = new Subscription();

  constructor(private route: ActivatedRoute, private router: Router) {}

  ngOnInit(): void {
    this.subs.add(
      this.route.paramMap.subscribe((paramMap: ParamMap) => {
        this.fsName = paramMap.get('fsName') ?? '';
        this.displayFsName = this.decodeFsName(this.fsName);
      })
    );
    this.updateActiveTab(this.router.url);
    this.subs.add(
      this.router.events
        .pipe(filter((event) => event instanceof NavigationEnd))
        .subscribe(() => this.updateActiveTab(this.router.url))
    );
  }

  ngOnDestroy(): void {
    this.subs.unsubscribe();
  }

  onSelected(tab: TABS): void {
    this.router.navigate(['/cephfs/mirroring', this.fsName, tab]);
  }

  get Tabs(): typeof TABS {
    return TABS;
  }

  private updateActiveTab(url: string): void {
    this.activeTab = Object.values(TABS).find((tab) => url.includes(`/${tab}`)) || TABS.overview;
  }

  private decodeFsName(fsName: string): string {
    if (!fsName) {
      return '';
    }
    try {
      return decodeURIComponent(fsName);
    } catch {
      return fsName;
    }
  }
}

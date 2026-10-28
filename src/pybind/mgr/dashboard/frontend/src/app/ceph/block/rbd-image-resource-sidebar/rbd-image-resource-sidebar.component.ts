import { Component, OnDestroy, OnInit } from '@angular/core';
import { ActivatedRoute, ParamMap } from '@angular/router';
import { Subscription } from 'rxjs';

import { SidebarItem } from '~/app/shared/components/sidebar-layout/sidebar-layout.component';
import { ImageSpec } from '~/app/shared/models/image-spec';
import { RbdImageResourceStateService } from '../../../shared/services/rbd-image-resource-state.service';

@Component({
  selector: 'cd-rbd-image-resource-sidebar',
  templateUrl: './rbd-image-resource-sidebar.component.html',
  styleUrls: ['./rbd-image-resource-sidebar.component.scss'],
  providers: [RbdImageResourceStateService],
  standalone: false
})
export class RbdImageResourceSidebarComponent implements OnInit, OnDestroy {
  private sub = new Subscription();
  readonly basePath = '/block/rbd';
  imageSpecRoute = '';
  imageName = '';
  sidebarItems: SidebarItem[] = [];

  constructor(
    private route: ActivatedRoute,
    private rbdImageResourceStateService: RbdImageResourceStateService
  ) {}

  ngOnInit(): void {
    this.sub.add(
      this.route.paramMap.subscribe((pm: ParamMap) => {
        this.imageSpecRoute = pm.get('image_spec') ?? '';
        this.buildSidebarItems();
        this.setFallbackImageName();
        this.rbdImageResourceStateService.load(this.imageSpecRoute);
      })
    );
  }

  ngOnDestroy(): void {
    this.sub.unsubscribe();
  }

  private setFallbackImageName(): void {
    if (!this.imageSpecRoute) {
      this.imageName = '';
      return;
    }

    try {
      const imageSpec = ImageSpec.fromString(decodeURIComponent(this.imageSpecRoute));
      this.imageName = imageSpec.imageName;
    } catch {
      this.imageName = this.imageSpecRoute;
    }
  }

  private buildSidebarItems(): void {
    this.sidebarItems = [
      {
        label: $localize`Overview`,
        route: [this.basePath, this.imageSpecRoute, 'overview'],
        routerLinkActiveOptions: { exact: true }
      },
      {
        label: $localize`Snapshots`,
        route: [this.basePath, this.imageSpecRoute, 'snapshots'],
        routerLinkActiveOptions: { exact: true }
      },
      {
        label: $localize`Configuration`,
        route: [this.basePath, this.imageSpecRoute, 'configuration'],
        routerLinkActiveOptions: { exact: true }
      },
      {
        label: $localize`Performance`,
        route: [this.basePath, this.imageSpecRoute, 'performance'],
        routerLinkActiveOptions: { exact: true }
      }
    ];
  }
}

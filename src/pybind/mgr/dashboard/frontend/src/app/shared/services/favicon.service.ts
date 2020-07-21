import { DOCUMENT } from '@angular/common';
import { Inject, Injectable, OnDestroy } from '@angular/core';

import { Subscription } from 'rxjs';

import { Color } from '../enum/color.enum';
import { SummaryService } from './summary.service';

@Injectable()
export class FaviconService implements OnDestroy {
  sub: Subscription;
  oldStatus: string;
  url: string;

  constructor(
    @Inject(DOCUMENT) private document: HTMLDocument,
    private summaryService: SummaryService
  ) {}

  init() {
    this.url = this.document.getElementById('cdFavicon')?.getAttribute('href');

    this.sub = this.summaryService.subscribe((summary) => {
      this.changeIcon(summary.health_status);
    });
  }

  changeIcon(status?: string) {
    if (status === this.oldStatus) {
      return;
    }

    this.oldStatus = status;

    const favicon = this.document.getElementById('cdFavicon');
    const faviconSize = 16;
    const radius = faviconSize / 4;

    const canvas = this.document.createElement('canvas');
    canvas.width = faviconSize;
    canvas.height = faviconSize;

    const context = canvas.getContext('2d');
    const img = this.document.createElement('img');
    img.src = this.url;

    img.onload = () => {
      // Draw Original Favicon as Background
      context.drawImage(img, 0, 0, faviconSize, faviconSize);

      // Cut notification circle area
      context.save();
      context.globalCompositeOperation = 'destination-out';
      context.beginPath();
      context.arc(canvas.width - radius, radius, radius + 2, 0, 2 * Math.PI);
      context.fill();
      context.restore();

      // Draw Notification Circle
      context.beginPath();
      context.arc(canvas.width - radius, radius, radius, 0, 2 * Math.PI);
      context.fillStyle = Color[status] || 'transparent';
      context.fill();

      // Replace favicon
      favicon.setAttribute('href', canvas.toDataURL('image/png'));
    };
  }

  ngOnDestroy() {
    this.changeIcon();
    this.sub?.unsubscribe();
  }
}

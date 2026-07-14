import {
  Component,
  OnInit,
  AfterViewChecked,
  HostListener,
  ElementRef
} from '@angular/core';
import { animate, style, transition, trigger } from '@angular/animations';
import { Router } from '@angular/router';
import { Observable } from 'rxjs';
import { ToastContent } from 'carbon-components-angular';
import { NotificationService } from '../../services/notification.service';

@Component({
  selector: 'cd-toast',
  templateUrl: './notification-toast.component.html',
  styleUrls: ['./notification-toast.component.scss'],
  animations: [
    trigger('toastAnimation', [
      transition(
        ':enter',
        [
          style({ opacity: 0, transform: 'translateX(100%)' }),
          animate('{{duration}} {{easing}}', style({ opacity: 1, transform: 'translateX(0)' }))
        ],
        { params: { duration: '240ms', easing: 'cubic-bezier(0.2, 0, 0.38, 0.9)' } }
      ),
      transition(
        ':leave',
        [
          style({ opacity: 1, transform: 'translateX(0)' }),
          animate('{{duration}} {{easing}}', style({ opacity: 0, transform: 'translateX(100%)' }))
        ],
        { params: { duration: '240ms', easing: 'cubic-bezier(0.2, 0, 0.38, 0.9)' } }
      )
    ])
  ],
  standalone: false
})
export class ToastComponent implements OnInit, AfterViewChecked {
  activeToasts$: Observable<ToastContent[]>;

  constructor(
    private notificationService: NotificationService,
    private router: Router,
    private el: ElementRef
  ) {}

  ngOnInit() {
    this.activeToasts$ = this.notificationService.activeToasts$;
  }

  ngAfterViewChecked() {
    const toasts = this.el.nativeElement.querySelectorAll('cds-toast');
    toasts.forEach((toast: HTMLElement) => {
      const subtitle = toast.querySelector('.cds--toast-notification__subtitle');
      const viewMore = toast.querySelector('.toast-view-more') as HTMLElement;
      if (!subtitle || !viewMore) return;
      const isTruncated = subtitle.scrollHeight > subtitle.clientHeight;
      viewMore.style.display = isTruncated ? '' : 'none';
    });
  }

  @HostListener('click', ['$event'])
  onViewMoreClick(event: Event) {
    const target = event.target as HTMLElement;
    if (target.classList.contains('toast-view-more')) {
      event.preventDefault();
      const href = target.getAttribute('href');
      if (href) {
        this.router.navigateByUrl(href.replace('#', ''));
      }
      this.notificationService.clearAllToasts();
    }
  }

  onToastClose(toast: ToastContent) {
    this.notificationService.removeToast(toast);
  }
}

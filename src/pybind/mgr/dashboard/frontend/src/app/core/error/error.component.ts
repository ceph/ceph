import { HttpClient } from '@angular/common/http';
import { Component, HostListener, OnDestroy, OnInit } from '@angular/core';
import { NavigationEnd, Router, RouterEvent } from '@angular/router';

import { Subscription } from 'rxjs';
import { filter } from 'rxjs/operators';

import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { DocService } from '~/app/shared/services/doc.service';
import { NotificationService } from '~/app/shared/services/notification.service';

@Component({
  selector: 'cd-error',
  templateUrl: './error.component.html',
  styleUrls: ['./error.component.scss']
})
export class ErrorComponent implements OnDestroy, OnInit {
  header: string;
  message: string;
  section: string;
  section_info: string;
  icon: string;
  docUrl: string;
  source: string;
  routerSubscription: Subscription;
  bootstrap: string;
  uiApiPath: string;
  button_route: string;
  button_name: string;

  constructor(private router: Router, private docService: DocService,
    private http: HttpClient,
    private notificationService: NotificationService, ) {}

  ngOnInit() {
    this.fetchData();
    this.routerSubscription = this.router.events
      .pipe(filter((event: RouterEvent) => event instanceof NavigationEnd))
      .subscribe(() => {
        this.fetchData();
      });
  }

  doBootstrap() {
    this.http.post(`ui-api/${this.uiApiPath}/configure`, {}).subscribe({
      next: () => {
        this.notificationService.show(
          NotificationType.info,
          'Configuring RBD Mirroring'
        );
      },
      error: (error: any) => {
        this.notificationService.show(
          NotificationType.error,
          error
        );
      },
      complete: () => {
        setTimeout(() => {
          this.router.navigate([this.uiApiPath]);
          this.notificationService.show(
            NotificationType.success,
            'Configured RBD Mirroring'
          );
        }, 3000);
      }
    });
  }

  @HostListener('window:beforeunload', ['$event']) unloadHandler(event: Event) {
    event.returnValue = false;
  }

  fetchData() {
    try {
      this.router.onSameUrlNavigation = 'reload';
      this.message = history.state.message;
      this.header = history.state.header;
      this.section = history.state.section;
      this.section_info = history.state.section_info;
      this.icon = history.state.icon;
      this.source = history.state.source;
      this.bootstrap = history.state.bootstrap;
      this.uiApiPath = history.state.uiApiPath;
      this.button_route = history.state.button_route;
      this.button_name = history.state.button_name;
      this.docUrl = this.docService.urlGenerator(this.section);
    } catch (error) {
      this.router.navigate(['/error']);
    }
  }

  ngOnDestroy() {
    if (this.routerSubscription) {
      this.routerSubscription.unsubscribe();
    }
  }
}

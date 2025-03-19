import { Injectable } from '@angular/core';
import { Router, NavigationEnd } from '@angular/router';
import { filter } from 'rxjs/operators';

@Injectable({
    providedIn: 'root'
})
export class ScrollRestorationService {
    constructor(private router: Router) {
        this.router.events
            .pipe(filter((event) => event instanceof NavigationEnd))
            .subscribe(() => {
                // Scroll window to top
                window.scrollTo({
                    top: 0,
                    behavior: 'smooth'
                });

                // Find and scroll main content container to top
                const mainContent = document.querySelector('.container-fluid');
                if (mainContent) {
                    mainContent.scrollTo({
                        top: 0,
                        behavior: 'smooth'
                    });
                }

                // Find and scroll any other scrollable containers to top
                const scrollableContainers = document.querySelectorAll('.overflow-auto, .table-scroller');
                scrollableContainers.forEach(container => {
                    if (container instanceof HTMLElement) {
                        container.scrollTop = 0;
                    }
                });
            });
    }
} 
import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';

import { AuthModule } from './auth/auth.module';
import { ForbiddenComponent } from './forbidden/forbidden.component';
import { MgrModulesModule } from './mgr-modules/mgr-modules.module';
import { NavigationModule } from './navigation/navigation.module';
import { NotFoundComponent } from './not-found/not-found.component';

@NgModule({
  imports: [CommonModule, NavigationModule, AuthModule, MgrModulesModule],
  exports: [NavigationModule],
  declarations: [NotFoundComponent, ForbiddenComponent]
})
export class CoreModule {}

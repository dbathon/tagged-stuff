import { Injectable } from '@angular/core';
import { CanActivate, CanActivateChild, ActivatedRouteSnapshot, RouterStateSnapshot, UrlTree, Router } from '@angular/router';
import { Observable } from 'rxjs';
import { SettingsService } from './settings.service';

@Injectable({
  providedIn: 'root'
})
export class SettingsAvailableGuard implements CanActivate, CanActivateChild {

  constructor(private settingsService: SettingsService, private router: Router) { }

  canActivate(next: ActivatedRouteSnapshot, state: RouterStateSnapshot): boolean {
    if (this.settingsService.settingsAvailable) {
      return true;
    }

    this.router.navigate(['/tagged-stuff/settings']);
    return false;
  }

  canActivateChild(next: ActivatedRouteSnapshot, state: RouterStateSnapshot): boolean {
    return this.canActivate(next, state);
  }

}

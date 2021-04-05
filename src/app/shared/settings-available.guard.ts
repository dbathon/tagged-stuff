import { Injectable } from '@angular/core';
import { CanActivate, CanActivateChild, ActivatedRouteSnapshot, RouterStateSnapshot, UrlTree, Router } from '@angular/router';
import { getSettings } from './settings';

@Injectable({
  providedIn: 'root'
})
export class SettingsAvailableGuard implements CanActivate, CanActivateChild {

  constructor(private router: Router) { }

  canActivate(next: ActivatedRouteSnapshot, state: RouterStateSnapshot): boolean {
    if (getSettings() !== undefined) {
      return true;
    }

    this.router.navigate(['/tagged-stuff/settings']);
    return false;
  }

  canActivateChild(next: ActivatedRouteSnapshot, state: RouterStateSnapshot): boolean {
    return this.canActivate(next, state);
  }

}

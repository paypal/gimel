import {
  ChangeDetectionStrategy, Component, ViewChildren, QueryList,
} from '@angular/core';
import {
  MdMenuTrigger, Overlay, OverlayRef, OverlayOrigin, OverlayState, Portal, TemplatePortalDirective,
} from '@angular/material';
import {Store} from '@ngrx/store';
import {Observable} from 'rxjs/Observable';

import {ConfigService} from '../../core/services';
import * as fromRoot from '../../core/store';
import {fadeInOutAnimation} from '../../shared/animations/animations';

@Component({
  selector: 'app-ghtoolbar', templateUrl: './ghtoolbar.component.html', styleUrls: ['./ghtoolbar.component.scss'], changeDetection: ChangeDetectionStrategy.OnPush, animations: [fadeInOutAnimation],
})
export class GhtoolbarComponent {
  @ViewChildren(MdMenuTrigger) triggers: QueryList<MdMenuTrigger>;
  @ViewChildren(TemplatePortalDirective) templatePortals: QueryList<Portal<any>>;
  @ViewChildren(OverlayOrigin) overlayOrigins: QueryList<OverlayOrigin>;

  private overlayRef: OverlayRef;

  showBanner$: Observable<any>;
  username: string;

  constructor(public overlay: Overlay, private config: ConfigService, private store: Store<fromRoot.State>) {
    this.username = this.config.userName;
    this.showBanner$ = this.store.select(fromRoot.getGlobalHeaderBanner);
  }

  openAccountMenu() {
    if (!this.triggers.last.menuOpen) {
      this.triggers.last.openMenu();
    }
  }

  openFeedbackMenu() {
    if (!this.triggers.first.menuOpen) {
      this.triggers.first.openMenu();
    }
  }

  openHelpMenu() {
    if (!this.overlayRef) {
      const overlayConfig = new OverlayState();
      overlayConfig.positionStrategy = this.overlay.position()
        .connectedTo(this.overlayOrigins.last.elementRef, {
          originX: 'end', originY: 'bottom',
        }, {overlayX: 'end', overlayY: 'top'});
      overlayConfig.hasBackdrop = true;
      overlayConfig.backdropClass = 'md-overlay-transparent-backdrop';
      this.overlayRef = this.overlay.create(overlayConfig);
    }

    this.overlayRef.attach(this.templatePortals.first);
    this.overlayRef.backdropClick().subscribe(() => this.overlayRef.detach());
  }
}

import {
  ChangeDetectionStrategy,
  Component,
  ViewChild,
  ViewChildren,
  QueryList,
  ViewContainerRef,
  Input,
  Output,
  EventEmitter
} from '@angular/core';
import {
  MdMenuTrigger,
  Overlay,
  OverlayRef,
  OverlayOrigin,
  OverlayState,
  Portal,
  TemplatePortalDirective,
} from '@angular/material';

import { ConfigService } from '../../core/services';

@Component({
  selector: 'app-ghsidebar',
  templateUrl: './ghsidebar.component.html',
  styleUrls: ['./ghsidebar.component.scss'],
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class GhsidebarComponent {
  @ViewChildren(MdMenuTrigger) triggers: QueryList<MdMenuTrigger>;
  @ViewChildren(TemplatePortalDirective) templatePortals: QueryList<Portal<any>>;
  @ViewChild(OverlayOrigin) overlayOrigin: OverlayOrigin;

  username: string;
  private overlayRef: OverlayRef;

  @Input() opened = false;
  @Input() sideNavOpened = false;
  @Input() xmarkState = false;

  @Output() onToggleSideNav = new EventEmitter<any>();

  constructor(public overlay: Overlay,
              public viewContainerRef: ViewContainerRef,
              private config: ConfigService) {
    this.username = this.config.userName;
  }

  toggleSideNav() {
    this.onToggleSideNav.emit();
  }

  openHelpMenu() {
    if (!this.overlayRef) {
      const overlayConfig = new OverlayState();
      overlayConfig.positionStrategy = this.overlay.position()
        .connectedTo(
          this.overlayOrigin.elementRef,
          { originX: 'start', originY: 'bottom' },
          { overlayX: 'start', overlayY: 'top' });
      overlayConfig.hasBackdrop = true;
      overlayConfig.backdropClass = 'md-overlay-transparent-backdrop';
      this.overlayRef = this.overlay.create(overlayConfig);
    }

    this.overlayRef.attach(this.templatePortals.first);
    this.overlayRef.backdropClick().subscribe(() => this.overlayRef.detach());
  }

  openAccountMenu() {
    if (!this.triggers.first.menuOpen) {
      this.triggers.first.openMenu();
    }
  }
}

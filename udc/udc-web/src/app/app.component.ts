import {Component, OnDestroy} from '@angular/core';
import {DomSanitizer} from '@angular/platform-browser';
import {MdIconRegistry, OverlayContainer} from '@angular/material';

@Component({
  selector: 'app-root', templateUrl: './app.component.html',
})
export class AppComponent implements OnDestroy {

  subscription: any;
  titleSub: any;

  constructor(private overlayContainer: OverlayContainer, mdIconRegistry: MdIconRegistry, sanitizer: DomSanitizer) {
    mdIconRegistry
      .addSvgIcon('jira', sanitizer.bypassSecurityTrustResourceUrl('assets/icons/jira.svg'))
      .addSvgIcon('slack', sanitizer.bypassSecurityTrustResourceUrl('assets/icons/slack.svg'))
      .addSvgIcon('github', sanitizer.bypassSecurityTrustResourceUrl('assets/icons/github-octocat.svg'))
      .addSvgIcon('confluence', sanitizer.bypassSecurityTrustResourceUrl('assets/icons/confluence.svg'));

    overlayContainer.themeClass = 'purple-light-theme';

  }

  ngOnDestroy() {
    this.subscription.unsubscribe();
    this.titleSub.unsubscribe();
  }
}

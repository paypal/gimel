import {
  ChangeDetectorRef,
  Directive,
  EmbeddedViewRef,
  Input,
  OnInit,
  OnDestroy,
  TemplateRef,
  ViewContainerRef,
} from '@angular/core';
import { Observable } from 'rxjs/Observable';

@Directive({
  selector: '[rxContext][rxContextOn]',
})
export class RxContextDirective implements OnInit, OnDestroy {
  @Input() rxContextOn: Observable<any>;

  private viewRef: EmbeddedViewRef<any>;
  private subscription: any;

  constructor(private viewContainer: ViewContainerRef,
              private template: TemplateRef<any>,
              private cdr: ChangeDetectorRef) {
  }

  ngOnInit() {
    if (this.rxContextOn) {
      this.subscription = this.rxContextOn.subscribe(state => {
        if (!this.viewRef) {
          this.viewRef = this.viewContainer
            .createEmbeddedView(this.template, { '$implicit': state });
        } else {
          this.viewRef.context.$implicit = state;
        }
        // this._cdr.detectChanges();
      });
    }
  }

  ngOnDestroy() {
    this.subscription.unsubscribe();
  }
}

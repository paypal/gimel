import { Component, Input, OnInit, OnDestroy } from '@angular/core';
import { Http, Response } from '@angular/http';
import { Observable, Subscription } from 'rxjs/Rx';

import 'rxjs/add/operator/do';
import 'rxjs/add/operator/delay';
import 'rxjs/add/operator/retryWhen';
import 'rxjs/add/operator/repeatWhen';
import 'rxjs/add/operator/takeWhile';

@Component({
  selector: 'health-check',
  templateUrl: './healthcheck.component.html',
  styleUrls: ['./healthcheck.component.css']
})
export class HealthCheckComponent implements OnInit, OnDestroy {
  @Input() pingUrl: string;
  @Input() pingIntervalMilli: number;
  @Input() title: string;

  private httpSubscription: Subscription;
  private svgSubscription: Subscription;

  private currentStatus: number | string;
  private success: number;
  private failure: number;

  private startAngle: number;
  private endAngle: number;
  private radius: number;
  private cx: number;
  private cy: number;
  private width: number;
  private color: string;
  private frameTime: number;
  private font: number;

  private RED: string = "#AA0000";
  private GREEN: string = "#00AA00";
  private BLUE: string = "#446688";


  constructor(private http: Http) {
    this.pingIntervalMilli = 3000;
    this.success = 0;
    this.failure = 0;

    this.startAngle = 1;
    this.endAngle = 360;
    this.cx = this.cy = 150;
    this.radius = 100;
    this.width = 40;
    this.color = this.RED;
    this.frameTime = 32;
    this.font = 35;
  }

  ngOnInit(): void {
    this.httpSubscription = this.http.get(this.pingUrl)
      .repeatWhen(() => Observable.timer(0, this.pingIntervalMilli))
      .retryWhen(err => {
        return err.do(res => {
          this.currentStatus = res.status === 0? "ERR": res.status;
          this.color = this.RED;
          this.failure++;
        }).delay(this.pingIntervalMilli)
      })
      .subscribe(res => {
        this.currentStatus = res.status;
        this.color = this.GREEN;
        this.success++;
      });

      this.svgSubscription = Observable.timer(0, this.frameTime)
        .takeWhile(val => val <= this.pingIntervalMilli/16)
        .repeatWhen(() => Observable.timer(0, this.pingIntervalMilli))
        .subscribe(() => {
          this.startAngle = this.startAngle +  Math.round((this.frameTime * 360)/this.pingIntervalMilli);
          if (this.startAngle > 360) {
            this.startAngle = this.startAngle - 360;
          }
        });
  }

  ngOnDestroy(): void {
    this.httpSubscription.unsubscribe();
    this.svgSubscription.unsubscribe();
  }

  //http://stackoverflow.com/questions/5736398/how-to-calculate-the-svg-path-for-an-arc-of-a-circle

  private describeArc(): string {
    var start = this.polarToCartesian(this.cx, this.cy, this.radius, this.endAngle);
    var end = this.polarToCartesian(this.cx, this.cy, this.radius, this.startAngle);

    var largeArcFlag = this.endAngle - this.startAngle <= 180 ? "0" : "1";

    var d = [
        "M", start.x, start.y, 
        "A", this.radius, this.radius, 0, largeArcFlag, 0, end.x, end.y
    ].join(" ");

    return d;   
  }

  private polarToCartesian(centerX, centerY, radius, angleInDegrees) {
    var angleInRadians = (angleInDegrees-90) * Math.PI / 180.0;
    return {
      x: centerX + (radius * Math.cos(angleInRadians)),
      y: centerY + (radius * Math.sin(angleInRadians))
    };
  }
}

package com.paypal.udc.entity.dataset.sampledata;

import java.util.List;


public class LivySessionResponse {
    private String appId;
    private long elapsedTime;
    private long endTime;
    private int id;
    private String kind;
    private String name;
    private String owner;
    private String proxyUser;
    private long startTime;
    private String state;
    private AppInfo appInfo;
    private List<String> log;

    public String getAppId() {
        return this.appId;
    }

    public void setAppId(final String appId) {
        this.appId = appId;
    }

    public long getElapsedTime() {
        return this.elapsedTime;
    }

    public void setElapsedTime(final long elapsedTime) {
        this.elapsedTime = elapsedTime;
    }

    public long getEndTime() {
        return this.endTime;
    }

    public void setEndTime(final long endTime) {
        this.endTime = endTime;
    }

    public int getId() {
        return this.id;
    }

    public void setId(final int id) {
        this.id = id;
    }

    public String getKind() {
        return this.kind;
    }

    public void setKind(final String kind) {
        this.kind = kind;
    }

    public String getName() {
        return this.name;
    }

    public void setName(final String name) {
        this.name = name;
    }

    public String getOwner() {
        return this.owner;
    }

    public void setOwner(final String owner) {
        this.owner = owner;
    }

    public String getProxyUser() {
        return this.proxyUser;
    }

    public void setProxyUser(final String proxyUser) {
        this.proxyUser = proxyUser;
    }

    public long getStartTime() {
        return this.startTime;
    }

    public void setStartTime(final long startTime) {
        this.startTime = startTime;
    }

    public String getState() {
        return this.state;
    }

    public void setState(final String state) {
        this.state = state;
    }

    public AppInfo getAppInfo() {
        return this.appInfo;
    }

    public void setAppInfo(final AppInfo appInfo) {
        this.appInfo = appInfo;
    }

    public List<String> getLog() {
        return this.log;
    }

    public void setLog(final List<String> log) {
        this.log = log;
    }

}

package com.paypal.udc.entity.dataset.sampledata;

import java.util.List;


public class LivySessionAvailabilityResponse {

    private int from;
    private int total;
    private List<LivySessionResponse> sessions;

    public int getFrom() {
        return this.from;
    }

    public void setFrom(final int from) {
        this.from = from;
    }

    public int getTotal() {
        return this.total;
    }

    public void setTotal(final int total) {
        this.total = total;
    }

    public List<LivySessionResponse> getSessions() {
        return this.sessions;
    }

    public void setSessions(final List<LivySessionResponse> sessions) {
        this.sessions = sessions;
    }

}

package com.paypal.udc.entity.dataset.sampledata;

import java.util.List;


public class LivyOutputResponse {

    private String status;
    private long execution_count;
    private SampleData data;
    private List<String> traceback;

    public List<String> getTraceback() {
        return this.traceback;
    }

    public void setTraceback(final List<String> traceback) {
        this.traceback = traceback;
    }

    public String getStatus() {
        return this.status;
    }

    public void setStatus(final String status) {
        this.status = status;
    }

    public long getExecution_count() {
        return this.execution_count;
    }

    public void setExecution_count(final long execution_count) {
        this.execution_count = execution_count;
    }

    public SampleData getData() {
        return this.data;
    }

    public void setData(final SampleData data) {
        this.data = data;
    }

}

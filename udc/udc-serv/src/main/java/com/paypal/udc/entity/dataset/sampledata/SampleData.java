package com.paypal.udc.entity.dataset.sampledata;

import com.fasterxml.jackson.annotation.JsonProperty;


public class SampleData {

    @JsonProperty(value = "text/plain")
    private String data;

    public String getData() {
        return this.data;
    }

    public void setData(final String data) {
        this.data = data;
    }

}

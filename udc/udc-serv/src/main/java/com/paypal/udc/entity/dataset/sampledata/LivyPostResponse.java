package com.paypal.udc.entity.dataset.sampledata;

public class LivyPostResponse {
    private long id;
    private String state;
    private LivyOutputResponse output;

    public long getId() {
        return this.id;
    }

    public void setId(final long id) {
        this.id = id;
    }

    public String getState() {
        return this.state;
    }

    public void setState(final String state) {
        this.state = state;
    }

    public LivyOutputResponse getOutput() {
        return this.output;
    }

    public void setOutput(final LivyOutputResponse output) {
        this.output = output;
    }

    @Override
    public String toString() {
        return "State => " + this.state + " Id => " + this.id + "Output => " + this.output.getStatus();
    }

}

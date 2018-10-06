package com.paypal.udc.util.enumeration;

public enum ActiveEnumeration {
    YES("Y", "Activate"),
    NO("N", "Deactivate"),
    PENDING("P", "Pending");

    private String flag;
    private final String description;

    private ActiveEnumeration(final String flag, final String description) {
        this.flag = flag;
        this.description = description;
    }

    public String getFlag() {
        return this.flag;
    }

    public void setFlag(final String flag) {
        this.flag = flag;
    }

    public String getDescription() {
        return this.description;
    }

}

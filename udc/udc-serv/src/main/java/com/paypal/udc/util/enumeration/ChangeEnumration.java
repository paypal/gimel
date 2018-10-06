package com.paypal.udc.util.enumeration;

public enum ChangeEnumration {
    CREATE("C", "Create"),
    DELETE("D", "Deactivate"),
    MODIFY("M", "Modify");

    private String flag;
    private final String description;

    private ChangeEnumration(final String flag, final String description) {
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

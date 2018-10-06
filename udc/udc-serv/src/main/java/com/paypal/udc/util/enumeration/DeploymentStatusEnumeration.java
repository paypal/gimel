package com.paypal.udc.util.enumeration;

public enum DeploymentStatusEnumeration {
    REGISTERING("Registering", "Registering"),
    APPROVED("Approved", "Deactivate"),
    REGISTERED("Registered", "Deployed"),
    PENDING("Pending Registration", "Pending Registration"),
    ERROR("Error in Creation", "Error in creation");

    private String flag;
    private final String description;

    private DeploymentStatusEnumeration(final String flag, final String description) {
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

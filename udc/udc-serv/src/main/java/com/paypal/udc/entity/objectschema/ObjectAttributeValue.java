package com.paypal.udc.entity.objectschema;

import java.io.Serializable;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Transient;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.ApiModelProperty;


@Entity
@Table(name = "pc_storage_object_attribute_value")
public class ObjectAttributeValue implements Serializable {

    private static final long serialVersionUID = 1L;

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @ApiModelProperty(notes = "The database generated object Attribute Value ID")
    @Column(name = "object_attribute_value_id")
    private long objectAttributeValueId;

    @ApiModelProperty(notes = "The database generated type attribute Key ID FK")
    @Column(name = "storage_ds_attribute_key_id")
    @NotNull
    private long storageDsAttributeKeyId;

    @ApiModelProperty(notes = "The database generated type object ID FK")
    @Column(name = "object_id")
    @NotNull
    private long objectId;

    @ApiModelProperty(notes = "Object Attribute Value")
    @Column(name = "object_attribute_value")
    @Size(min = 1, message = "name need to have atleast 1 character")
    @NotNull
    private String objectAttributeValue;

    @ApiModelProperty(notes = "Is this Property Customized?")
    @Column(name = "is_customized")
    @Size(min = 1, message = "name need to have atleast 1 character")
    @NotNull
    private String isCustomized;

    @ApiModelProperty(notes = "Is the Property Active ?")
    @Column(name = "is_active_y_n")
    @JsonIgnore
    private String isActiveYN;

    @ApiModelProperty(notes = "Created User")
    @Column(name = "cre_user")
    @JsonIgnore
    private String createdUser;

    @ApiModelProperty(notes = "Created Timestamp")
    @Column(name = "cre_ts")
    @JsonIgnore
    private String createdTimestamp;

    @ApiModelProperty(notes = "Updated User")
    @Column(name = "upd_user")
    @JsonIgnore
    private String updatedUser;

    @ApiModelProperty(notes = "Updated Timestamp")
    @Column(name = "upd_ts")
    @JsonIgnore
    private String updatedTimestamp;

    @Transient
    private String storageDsAttributeKeyName;

    public String getIsCustomized() {
        return this.isCustomized;
    }

    public void setIsCustomized(final String isCustomized) {
        this.isCustomized = isCustomized;
    }

    public long getObjectId() {
        return this.objectId;
    }

    public long getObjectAttributeValueId() {
        return this.objectAttributeValueId;
    }

    public void setObjectAttributeValueId(final long objectAttributeValueId) {
        this.objectAttributeValueId = objectAttributeValueId;
    }

    public long getStorageDsAttributeKeyId() {
        return this.storageDsAttributeKeyId;
    }

    public void setStorageDsAttributeKeyId(final long storageDsAttributeKeyId) {
        this.storageDsAttributeKeyId = storageDsAttributeKeyId;
    }

    public String getObjectAttributeValue() {
        return this.objectAttributeValue;
    }

    public void setObjectAttributeValue(final String objectAttributeValue) {
        this.objectAttributeValue = objectAttributeValue;
    }

    @JsonIgnore
    public String getIsActiveYN() {
        return this.isActiveYN;
    }

    @JsonProperty
    public void setIsActiveYN(final String isActiveYN) {
        this.isActiveYN = isActiveYN;
    }

    public void setObjectId(final long objectId) {
        this.objectId = objectId;
    }

    @JsonIgnore
    public String getCreatedUser() {
        return this.createdUser;
    }

    @JsonProperty
    public void setCreatedUser(final String createdUser) {
        this.createdUser = createdUser;
    }

    @JsonIgnore
    public String getCreatedTimestamp() {
        return this.createdTimestamp;
    }

    @JsonProperty
    public void setCreatedTimestamp(final String createdTimestamp) {
        this.createdTimestamp = createdTimestamp;
    }

    @JsonIgnore
    public String getUpdatedUser() {
        return this.updatedUser;
    }

    @JsonProperty
    public void setUpdatedUser(final String updatedUser) {
        this.updatedUser = updatedUser;
    }

    @JsonIgnore
    public String getUpdatedTimestamp() {
        return this.updatedTimestamp;
    }

    @JsonProperty
    public void setUpdatedTimestamp(final String updatedTimestamp) {
        this.updatedTimestamp = updatedTimestamp;
    }

    public String getStorageDsAttributeKeyName() {
        return this.storageDsAttributeKeyName;
    }

    public void setStorageDsAttributeKeyName(final String storageDsAttributeKeyName) {
        this.storageDsAttributeKeyName = storageDsAttributeKeyName;
    }

    @Override
    public String toString() {
        return "Type Attribute ID == " + this.storageDsAttributeKeyId + "Object Id == " + this.objectId
                + "Object Attribute Value = " + this.objectAttributeValue;
    }

}

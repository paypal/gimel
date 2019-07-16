package com.paypal.udc.entity.classification;

import java.io.Serializable;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;
import io.swagger.annotations.ApiModelProperty;


@Entity
@Table(name = "pc_classifications_attributes")
public class ClassificationAttribute implements Serializable {

    private static final long serialVersionUID = 1L;

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @ApiModelProperty(notes = "The database generated dataset classification attribute ID")
    @Column(name = "dataset_classification_attribute_id")
    @NotNull
    private long datasetClassificationMapId;

    @ApiModelProperty(notes = "The database generated dataset classification ID")
    @Column(name = "dataset_classification_id")
    @NotNull
    private long datasetClassificationId;

    @ApiModelProperty(notes = "sample size")
    @Column(name = "sample_size")
    @NotNull
    private String sampleSize;

    @ApiModelProperty(notes = "True Positive")
    @Column(name = "true_positive")
    @NotNull
    private String truePositive;

    @ApiModelProperty(notes = "Confidence Score")
    @Column(name = "confidence_score")
    @NotNull
    private String confidenceScore;

    @ApiModelProperty(notes = "Alert Create Date")
    @Column(name = "alert_create_date")
    @NotNull
    private String alertCreateDate;

    @ApiModelProperty(notes = "Is Encrypted")
    @Column(name = "is_encrypted")
    @NotNull
    private String isEncrypted;

    @ApiModelProperty(notes = "Encryption Type")
    @Column(name = "encryption_type")
    @NotNull
    private String encryptionType;

    @ApiModelProperty(notes = "Classifier Model Version")
    @Column(name = "classifier_model_version")
    @NotNull
    private String classifierModelVersion;

    @ApiModelProperty(notes = "Classifier Model")
    @Column(name = "classifier_model")
    @NotNull
    private String classifierModel;

    public long getDatasetClassificationMapId() {
        return this.datasetClassificationMapId;
    }

    public void setDatasetClassificationMapId(final long orionDatasetClassificationMapId) {
        this.datasetClassificationMapId = orionDatasetClassificationMapId;
    }

    public long getDatasetClassificationId() {
        return this.datasetClassificationId;
    }

    public void setDatasetClassificationId(final long datasetClassificationId) {
        this.datasetClassificationId = datasetClassificationId;
    }

    public String getSampleSize() {
        return this.sampleSize;
    }

    public void setSampleSize(final String sampleSize) {
        this.sampleSize = sampleSize;
    }

    public String getTruePositive() {
        return this.truePositive;
    }

    public void setTruePositive(final String truePositive) {
        this.truePositive = truePositive;
    }

    public String getConfidenceScore() {
        return this.confidenceScore;
    }

    public void setConfidenceScore(final String confidenceScore) {
        this.confidenceScore = confidenceScore;
    }

    public String getAlertCreateDate() {
        return this.alertCreateDate;
    }

    public void setAlertCreateDate(final String alertCreateDate) {
        this.alertCreateDate = alertCreateDate;
    }

    public String getIsEncrypted() {
        return this.isEncrypted;
    }

    public void setIsEncrypted(final String isEncrypted) {
        this.isEncrypted = isEncrypted;
    }

    public String getEncryptionType() {
        return this.encryptionType;
    }

    public void setEncryptionType(final String encryptionType) {
        this.encryptionType = encryptionType;
    }

    public String getClassifierModelVersion() {
        return this.classifierModelVersion;
    }

    public void setClassifierModelVersion(final String classifierModelVersion) {
        this.classifierModelVersion = classifierModelVersion;
    }

    public String getClassifierModel() {
        return this.classifierModel;
    }

    public void setClassifierModel(final String classifierModel) {
        this.classifierModel = classifierModel;
    }

    public ClassificationAttribute() {

    }

    public ClassificationAttribute(@NotNull final long datasetClassificationId, @NotNull final String sampleSize,
            @NotNull final String truePositive, @NotNull final String confidenceScore,
            @NotNull final String alertCreateDate, @NotNull final String isEncrypted,
            @NotNull final String encryptionType, @NotNull final String classifierModelVersion,
            @NotNull final String classifierModel) {
        this.datasetClassificationId = datasetClassificationId;
        this.sampleSize = sampleSize;
        this.truePositive = truePositive;
        this.confidenceScore = confidenceScore;
        this.alertCreateDate = alertCreateDate;
        this.isEncrypted = isEncrypted;
        this.encryptionType = encryptionType;
        this.classifierModelVersion = classifierModelVersion;
        this.classifierModel = classifierModel;
    }

    public ClassificationAttribute(@NotNull final long datasetClassificationMapId,
            @NotNull final long datasetClassificationId, @NotNull final String sampleSize,
            @NotNull final String truePositive, @NotNull final String confidenceScore,
            @NotNull final String alertCreateDate, @NotNull final String isEncrypted,
            @NotNull final String encryptionType, @NotNull final String classifierModelVersion,
            @NotNull final String classifierModel) {
        this.datasetClassificationMapId = datasetClassificationMapId;
        this.datasetClassificationId = datasetClassificationId;
        this.sampleSize = sampleSize;
        this.truePositive = truePositive;
        this.confidenceScore = confidenceScore;
        this.alertCreateDate = alertCreateDate;
        this.isEncrypted = isEncrypted;
        this.encryptionType = encryptionType;
        this.classifierModelVersion = classifierModelVersion;
        this.classifierModel = classifierModel;
    }

}

package com.paypal.udc.validator.cluster;

import com.paypal.udc.entity.Cluster;
import com.paypal.udc.exception.ValidationError;


public interface ClusterValidator {

    void setNextChain(ClusterValidator nextChain);

    void validate(Cluster storage, final Cluster updatedStorage) throws ValidationError;
}

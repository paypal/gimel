package com.paypal.udc.validator.dataset;

import com.paypal.udc.entity.dataset.Dataset;
import com.paypal.udc.exception.ValidationError;


public interface DatasetValidator {

    void setNextChain(DatasetValidator nextChain);

    void validate(Dataset dataSet, final Dataset updatedDataSet) throws ValidationError;
}

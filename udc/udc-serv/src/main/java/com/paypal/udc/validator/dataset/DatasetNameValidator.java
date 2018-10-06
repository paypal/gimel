package com.paypal.udc.validator.dataset;

import org.springframework.stereotype.Component;
import com.paypal.udc.entity.dataset.Dataset;
import com.paypal.udc.exception.ValidationError;


@Component
public class DatasetNameValidator implements DatasetValidator {

    DatasetValidator chain;

    @Override
    public void setNextChain(final DatasetValidator nextChain) {
        this.chain = nextChain;
    }

    @Override
    public void validate(final Dataset dataSet, final Dataset updatedDataSet) throws ValidationError {
        if (dataSet.getStorageDataSetName() != null && dataSet.getStorageDataSetName().length() >= 0) {
            updatedDataSet.setStorageDataSetName(dataSet.getStorageDataSetName());
        }
        this.chain.validate(dataSet, updatedDataSet);
    }

}

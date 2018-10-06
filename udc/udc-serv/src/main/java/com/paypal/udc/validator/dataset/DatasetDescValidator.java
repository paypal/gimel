package com.paypal.udc.validator.dataset;

import org.springframework.stereotype.Component;
import com.paypal.udc.entity.dataset.Dataset;
import com.paypal.udc.exception.ValidationError;


@Component
public class DatasetDescValidator implements DatasetValidator {

    DatasetValidator chain;

    @Override
    public void setNextChain(final DatasetValidator nextChain) {
        this.chain = nextChain;
    }

    @Override
    public void validate(final Dataset dataSet, final Dataset updatedDataSet) throws ValidationError {
        if (dataSet.getStorageDataSetDescription() != null
                && dataSet.getStorageDataSetDescription().length() >= 0) {
            updatedDataSet.setStorageDataSetDescription(dataSet.getStorageDataSetDescription());
        }
        this.chain.validate(dataSet, updatedDataSet);

    }
}

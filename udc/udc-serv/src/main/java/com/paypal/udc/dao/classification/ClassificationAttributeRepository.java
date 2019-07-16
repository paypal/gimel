package com.paypal.udc.dao.classification;

import org.springframework.data.repository.CrudRepository;
import com.paypal.udc.entity.classification.ClassificationAttribute;


public interface ClassificationAttributeRepository extends CrudRepository<ClassificationAttribute, Long> {

}

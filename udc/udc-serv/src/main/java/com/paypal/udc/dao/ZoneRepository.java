package com.paypal.udc.dao;

import org.springframework.data.repository.CrudRepository;
import com.paypal.udc.entity.Zone;


public interface ZoneRepository extends CrudRepository<Zone, Long> {

    public Zone findByZoneName(String zoneName);

}

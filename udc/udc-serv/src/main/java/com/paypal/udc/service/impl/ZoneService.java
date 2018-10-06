package com.paypal.udc.service.impl;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import javax.validation.ConstraintViolationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.TransactionSystemException;
import com.paypal.udc.dao.ZoneRepository;
import com.paypal.udc.entity.Zone;
import com.paypal.udc.exception.ValidationError;
import com.paypal.udc.service.IZoneService;
import com.paypal.udc.util.UserUtil;
import com.paypal.udc.util.enumeration.ActiveEnumeration;
import com.paypal.udc.validator.zone.ZoneDescValidator;
import com.paypal.udc.validator.zone.ZoneNameValidator;


@Service
public class ZoneService implements IZoneService {

    @Autowired
    private ZoneRepository zoneRepository;
    @Autowired
    private UserUtil userUtil;
    @Autowired
    private ZoneNameValidator s1;
    @Autowired
    private ZoneDescValidator s2;

    final static Logger logger = LoggerFactory.getLogger(ClusterService.class);
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss");

    @Override
    public List<Zone> getAllZones() {
        final List<Zone> zones = new ArrayList<Zone>();
        this.zoneRepository.findAll().forEach(zone -> zones.add(zone));
        return zones;
    }

    @Override
    public Zone getZoneById(final long zoneId) {
        return this.zoneRepository.findOne(zoneId);
    }

    @Override
    public Zone addZone(final Zone zone) throws ValidationError {
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final ValidationError v = new ValidationError();
        try {
            final String createdUser = zone.getCreatedUser();
            this.userUtil.validateUser(createdUser);
            zone.setUpdatedUser(zone.getCreatedUser());
            zone.setCreatedTimestamp(sdf.format(timestamp));
            zone.setUpdatedTimestamp(sdf.format(timestamp));
            zone.setIsActiveYN(ActiveEnumeration.YES.getFlag());
            final Zone insertedZone = this.zoneRepository.save(zone);
            return insertedZone;
        }
        catch (final ConstraintViolationException e) {
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("Zone name is empty");
            throw v;
        }
        catch (final DataIntegrityViolationException e) {
            v.setErrorCode(HttpStatus.CONFLICT);
            v.setErrorDescription("Zone name is duplicated");
            throw v;
        }
    }

    @Override
    public Zone updateZone(final Zone zone) throws ValidationError {
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final ValidationError v = new ValidationError();
        Zone tempZone = this.zoneRepository.findOne(zone.getZoneId());
        if (tempZone != null) {
            try {
                tempZone.setUpdatedUser(zone.getUpdatedUser());
                tempZone.setUpdatedTimestamp(sdf.format(timestamp));
                this.s1.setNextChain(this.s2);
                this.s1.validate(zone, tempZone);
                tempZone = this.zoneRepository.save(tempZone);
                return tempZone;
            }
            catch (final TransactionSystemException e) {
                v.setErrorCode(HttpStatus.BAD_REQUEST);
                v.setErrorDescription("Zone name is empty");
                throw v;
            }
            catch (final DataIntegrityViolationException e) {
                v.setErrorCode(HttpStatus.CONFLICT);
                v.setErrorDescription("Zone name is duplicated");
                throw v;
            }
        }
        else {
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("Zone ID is invalid");
            throw v;
        }

    }

    @Override
    public Zone deActivateZone(final long zoneId) throws ValidationError {
        final ValidationError v = new ValidationError();
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        Zone tempZone = this.zoneRepository.findOne(zoneId);
        if (tempZone != null) {
            tempZone.setUpdatedTimestamp(sdf.format(timestamp));
            tempZone.setIsActiveYN(ActiveEnumeration.NO.getFlag());
            tempZone = this.zoneRepository.save(tempZone);
            return tempZone;
        }
        else {
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("Zone ID is invalid");
            throw v;
        }
    }

    @Override
    public Zone getZoneByName(final String zoneName) {
        return this.zoneRepository.findByZoneName(zoneName);
    }

    @Override
    public Zone reActivateZone(final long zoneId) throws ValidationError {
        final ValidationError v = new ValidationError();
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        Zone tempZone = this.zoneRepository.findOne(zoneId);
        if (tempZone != null) {
            tempZone.setUpdatedTimestamp(sdf.format(timestamp));
            tempZone.setIsActiveYN(ActiveEnumeration.YES.getFlag());
            tempZone = this.zoneRepository.save(tempZone);
            return tempZone;
        }
        else {
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("Zone ID is invalid");
            throw v;
        }
    }

}

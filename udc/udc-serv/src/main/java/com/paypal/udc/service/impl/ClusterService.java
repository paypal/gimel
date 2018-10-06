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
import com.paypal.udc.dao.ClusterRepository;
import com.paypal.udc.entity.Cluster;
import com.paypal.udc.exception.ValidationError;
import com.paypal.udc.service.IClusterService;
import com.paypal.udc.util.UserUtil;
import com.paypal.udc.util.enumeration.ActiveEnumeration;
import com.paypal.udc.validator.cluster.ClusterDescValidator;
import com.paypal.udc.validator.cluster.ClusterLivyEndpointValidator;
import com.paypal.udc.validator.cluster.ClusterLivyPortValidator;
import com.paypal.udc.validator.cluster.ClusterNameValidator;


@Service
public class ClusterService implements IClusterService {
    @Autowired
    private ClusterRepository clusterRepository;

    @Autowired
    private ClusterDescValidator s2;

    @Autowired
    private ClusterLivyEndpointValidator s4;

    @Autowired
    private ClusterLivyPortValidator s3;

    @Autowired
    private ClusterNameValidator s1;

    @Autowired
    private UserUtil userUtil;

    final static Logger logger = LoggerFactory.getLogger(ClusterService.class);
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss");

    @Override
    public List<Cluster> getAllClusters() {
        final List<Cluster> clusters = new ArrayList<Cluster>();
        this.clusterRepository.findAll().forEach(
                cluster -> {
                    clusters.add(cluster);
                });
        return clusters;
    }

    @Override
    public Cluster getClusterById(final long clusterId) {
        return this.clusterRepository.findOne(clusterId);
    }

    @Override
    public Cluster addCluster(final Cluster cluster) throws ValidationError {
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final ValidationError v = new ValidationError();
        try {
            final String createdUser = cluster.getCreatedUser();
            this.userUtil.validateUser(createdUser);
            cluster.setUpdatedUser(cluster.getCreatedUser());
            cluster.setCreatedTimestamp(sdf.format(timestamp));
            cluster.setUpdatedTimestamp(sdf.format(timestamp));
            cluster.setIsActiveYN(ActiveEnumeration.YES.getFlag());
            final Cluster insertedCluster = this.clusterRepository.save(cluster);
            return insertedCluster;
        }
        catch (final ConstraintViolationException e) {
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("Cluster name is empty");
            throw v;
        }
        catch (final DataIntegrityViolationException e) {
            v.setErrorCode(HttpStatus.CONFLICT);
            v.setErrorDescription("Cluster name is duplicated");
            throw v;
        }
    }

    @Override
    public Cluster updateCluster(final Cluster cluster) throws ValidationError {
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        final ValidationError v = new ValidationError();
        Cluster tempCluster = this.clusterRepository.findOne(cluster.getClusterId());
        if (tempCluster != null) {
            try {
                tempCluster.setUpdatedUser(cluster.getCreatedUser());
                tempCluster.setUpdatedTimestamp(sdf.format(timestamp));
                this.s1.setNextChain(this.s3);
                this.s3.setNextChain(this.s4);
                this.s4.setNextChain(this.s2);
                this.s1.validate(cluster, tempCluster);
                tempCluster = this.clusterRepository.save(tempCluster);
                return tempCluster;
            }
            catch (final TransactionSystemException e) {
                v.setErrorCode(HttpStatus.BAD_REQUEST);
                v.setErrorDescription("Cluster name is empty");
                throw v;
            }
            catch (final DataIntegrityViolationException e) {
                v.setErrorCode(HttpStatus.CONFLICT);
                v.setErrorDescription("Cluster name is duplicated");
                throw v;
            }
        }
        else {
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("Cluster ID is invalid");
            throw v;
        }

    }

    @Override
    public Cluster deActivateCluster(final long clusterId) throws ValidationError {
        final ValidationError v = new ValidationError();
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        Cluster tempCluster = this.clusterRepository.findOne(clusterId);
        if (tempCluster != null) {
            tempCluster.setUpdatedTimestamp(sdf.format(timestamp));
            tempCluster.setIsActiveYN(ActiveEnumeration.NO.getFlag());
            tempCluster = this.clusterRepository.save(tempCluster);
            return tempCluster;
        }
        else {
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("Cluster ID is invalid");
            throw v;
        }
    }

    @Override
    public Cluster getClusterByName(final String clusterName) {
        return this.clusterRepository.findByClusterName(clusterName);
    }

    @Override
    public Cluster reActivateCluster(final long clusterId) throws ValidationError {
        final ValidationError v = new ValidationError();
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        Cluster tempCluster = this.clusterRepository.findOne(clusterId);
        if (tempCluster != null) {
            tempCluster.setUpdatedTimestamp(sdf.format(timestamp));
            tempCluster.setIsActiveYN(ActiveEnumeration.YES.getFlag());
            tempCluster = this.clusterRepository.save(tempCluster);
            return tempCluster;
        }
        else {
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("Cluster ID is invalid");
            throw v;
        }
    }

}

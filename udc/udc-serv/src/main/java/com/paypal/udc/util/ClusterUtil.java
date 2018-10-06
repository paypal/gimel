package com.paypal.udc.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import com.paypal.udc.dao.ClusterRepository;
import com.paypal.udc.dao.objectschema.ObjectSchemaMapRepository;
import com.paypal.udc.dao.storagesystem.StorageSystemRepository;
import com.paypal.udc.entity.Cluster;
import com.paypal.udc.entity.objectschema.ObjectSchemaMap;
import com.paypal.udc.entity.storagesystem.StorageSystem;
import com.paypal.udc.exception.ValidationError;


@Component
public class ClusterUtil {

    @Autowired
    private ClusterRepository clusterRepository;

    @Autowired
    private ObjectSchemaMapRepository objectSchemaMapRepository;

    @Autowired
    private StorageSystemRepository storageSystemRepository;

    public void validateCluster(final Long clusterId) throws ValidationError {
        final Cluster cluster = this.clusterRepository.findOne(clusterId);
        if (cluster == null) {
            final ValidationError v = new ValidationError();
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("ClusterID is incorrect");
            throw v;
        }
    }

    public void validateClusters(final List<Long> clusters) throws ValidationError {

        if (clusters == null || clusters.size() == 0) {
            final ValidationError v = new ValidationError();
            v.setErrorCode(HttpStatus.BAD_REQUEST);
            v.setErrorDescription("Clusters not supplied");
            throw v;
        }
        for (final Long clusterId : clusters) {
            this.validateCluster(clusterId);
        }
    }

    public Map<Long, Cluster> getClusters() {
        final Map<Long, Cluster> clusters = new HashMap<Long, Cluster>();
        this.clusterRepository.findAll().forEach(cluster -> clusters.put(cluster.getClusterId(), cluster));
        return clusters;
    }

    public List<Cluster> getAllClusters() {
        final List<Cluster> clusters = new ArrayList<Cluster>();
        this.clusterRepository.findAll().forEach(cluster -> clusters.add(cluster));
        return clusters;
    }

    public Cluster getClusterByObjectId(final long objectId) throws ValidationError {
        final ObjectSchemaMap schemaMap = this.objectSchemaMapRepository.findOne(objectId);
        if (schemaMap == null) {
            final ValidationError verror = new ValidationError();
            verror.setErrorCode(HttpStatus.BAD_REQUEST);
            verror.setErrorDescription("Invalid Object ID");
            throw verror;
        }
        final StorageSystem storageSystem = this.storageSystemRepository.findOne(schemaMap.getStorageSystemId());
        final long runningClusterId = storageSystem.getRunningClusterId();
        final Cluster cluster = this.clusterRepository.findOne(runningClusterId);
        return cluster;
    }

}

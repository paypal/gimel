package com.paypal.udc.service;

import java.util.List;
import com.paypal.udc.entity.Cluster;
import com.paypal.udc.exception.ValidationError;


public interface IClusterService {

    List<Cluster> getAllClusters();

    Cluster getClusterById(long clusterId);

    Cluster addCluster(Cluster cluster) throws ValidationError;

    Cluster updateCluster(Cluster cluster) throws ValidationError;

    Cluster deActivateCluster(long clusterId) throws ValidationError;

    Cluster getClusterByName(String name);

    Cluster reActivateCluster(long clusterId) throws ValidationError;

}

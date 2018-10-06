package com.paypal.udc.dao;

import org.springframework.data.repository.CrudRepository;
import com.paypal.udc.entity.Cluster;


public interface ClusterRepository extends CrudRepository<Cluster, Long> {

    public Cluster findByClusterName(String clusterName);

    public Cluster findFirstByOrderByClusterId();

}

package com.paypal.udc;

import javax.sql.DataSource;
import org.springframework.boot.autoconfigure.jdbc.DataSourceBuilder;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
public class UDCDataSource {

    // @Bean(name = "metastoreDb")
    @Bean
    @ConfigurationProperties(prefix = "spring.datasource")
    public DataSource dataSource() {
        return DataSourceBuilder.create().build();
    }

    // @Bean(name = "hivestoreDb")
    // @ConfigurationProperties(prefix = "spring.secondDataSource")
    // public DataSource heliosHiveDataSource() {
    // return DataSourceBuilder.create().build();
    // }
    //
    // @Bean(name = "metastoreJdbcTemplate")
    // public JdbcTemplate metaStoreTemplate(@Qualifier("metastoreDb") final DataSource dsMySQL) {
    // return new JdbcTemplate(dsMySQL);
    // }
    //
    // @Bean(name = "hivestoreJdbcTemplate")
    // public JdbcTemplate hiveStoreTemplate(@Qualifier("hivestoreDb") final DataSource dsMySQL) {
    // return new JdbcTemplate(dsMySQL);
    // }

}

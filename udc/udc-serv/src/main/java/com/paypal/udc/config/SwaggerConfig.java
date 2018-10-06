package com.paypal.udc.config;

//import static springfox.documentation.builders.PathSelectors.regex;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.bind.annotation.CrossOrigin;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.service.ApiInfo;
import springfox.documentation.service.Contact;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;


@Configuration
@EnableSwagger2
@CrossOrigin(origins = "*")
public class SwaggerConfig {
    @Bean
    public Docket productApi() {
        return new Docket(DocumentationType.SWAGGER_2)
                .select()
                .apis(RequestHandlerSelectors.basePackage("com.paypal.udc.controller"))
                .build().apiInfo(this.metaData());
    }

    private ApiInfo metaData() {
        final ApiInfo apiInfo = new ApiInfo(
                "Unified Data Catalog Services",
                "Unified Data Catalog Services for CRUD operations on MySQL",
                "1.0",
                "Terms of service",
                new Contact(
                        "udc",
                        "http://gimel.io",
                        "https://groups.google.com/forum/#!forum/gimel-user"),
                "", "");
        return apiInfo;
    }
}

package com.yolo.estemplate.domain;

import lombok.Data;
import org.springframework.data.elasticsearch.annotations.Document;

import java.util.Date;

//@Document(indexName = "twitter",type="_doc")
@Data
@Document(indexName = "twitter1",type = "_doc")
public class User {
    private String user;
    private Integer uid;
    private String city;
    private String province;
    private String country;
    private Date dt;
}

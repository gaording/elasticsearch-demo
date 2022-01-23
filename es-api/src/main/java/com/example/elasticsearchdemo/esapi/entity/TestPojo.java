package com.example.elasticsearchdemo.esapi.entity;

import com.alibaba.fastjson.annotation.JSONField;

import java.time.LocalDateTime;

/**
 * @program: elasticsearch-demo
 * @date: 2022/1/21
 * @author: gaorunding1
 * @description:
 **/
public class TestPojo {
    private String user;
    private Integer age;
    private String content;

    @JSONField(format = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime date;

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public LocalDateTime getDate() {
        return date;
    }

    public void setDate(LocalDateTime date) {
        this.date = date;
    }
}

package com.supershan.es;

public class ElasticsearchException extends RuntimeException {
    public ElasticsearchException(String s, Exception e) {
        super(s, e);
    }
    public ElasticsearchException(String s){
        super(s);
    }
}
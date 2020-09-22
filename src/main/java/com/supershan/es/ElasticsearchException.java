package com.supershan.es;

/**
 *
 * @author mac
 */
public class ElasticsearchException extends RuntimeException {
    public ElasticsearchException(String s, Exception e) {
        super(s, e);
    }
    public ElasticsearchException(String s){
        super(s);
    }
}
package com.vaishali.kafka.connect.common;

/**
 * Created by vikas.tikoo on 7/12/17.
 */
public interface Operation<T> {
    T doIt() throws Exception;
}

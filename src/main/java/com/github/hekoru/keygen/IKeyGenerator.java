package com.github.hekoru.keygen;

/**
 * Created by IntelliJ IDEA.
 * User: hector
 * Date: 20/04/11
 * Time: 11:06
 * To change this template use File | Settings | File Templates.
 */
public interface IKeyGenerator {

    long getId(String resource) throws Exception;
}

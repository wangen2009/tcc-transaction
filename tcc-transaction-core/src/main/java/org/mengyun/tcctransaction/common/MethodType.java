package org.mengyun.tcctransaction.common;

/**
 * 方法类型
 *
 * Created by changmingxie on 11/11/15.
 */
public enum  MethodType {

    /**
     * 根
     */
    ROOT,
    @Deprecated
    CONSUMER,
    /**
     * 提供者
     */
    PROVIDER,
    /**
     * 常规
     */
    NORMAL
}

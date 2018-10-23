package com.seven.spark.hbase.rowkey;

import java.io.Serializable;

/**
 * Created by IntelliJ IDEA.
 * author   seven
 * email    straymax@163.com
 * date     2018/5/10 下午1:37
 *
 * RowKey生成器,RowKey设计三原则
 * <li>长度</li>
 * <li>散列</li>
 * <li>唯一</li>
 */
public interface RowKeyGenerator<T> extends Serializable {

    /**
     * 生成RowKey
     *
     * @return RowKey
     */
    byte[] generate(T t);
}

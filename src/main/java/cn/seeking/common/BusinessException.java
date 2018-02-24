package cn.seeking.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author wangpeiyong
 * @Date 2018/1/15 16:23
 */
public class BusinessException extends RuntimeException {
    private static Logger logger = LoggerFactory.getLogger(BusinessException.class);

    public BusinessException(Throwable e) {
        super(e);
        logger.error("error:", e);
    }

    public BusinessException(String msg) {
        super(msg);
        logger.error(msg);
    }

    public BusinessException(String msg, Throwable e) {
        super(msg, e);
        logger.error(msg, e);
    }
}

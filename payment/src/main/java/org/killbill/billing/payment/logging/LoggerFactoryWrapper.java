package org.killbill.billing.payment.logging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggerFactoryWrapper {

    public Logger getLogger(Class<?> clazz) {
        return LoggerFactory.getLogger(clazz);
    }
}

/*
 * Copyright 2010-2013 Ning, Inc.
 * Copyright 2014-2017 Groupon, Inc
 * Copyright 2014-2017 The Billing Project, LLC
 *
 * The Billing Project licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.killbill.billing.payment.core;

import org.killbill.billing.callcontext.InternalCallContext;
import org.killbill.billing.payment.api.PaymentApiException;
import org.killbill.billing.payment.dao.PaymentAttemptModelDao;
import org.killbill.billing.payment.dao.PaymentTransactionModelDao;
import org.killbill.billing.payment.glue.DefaultPaymentService;
import org.killbill.billing.payment.logging.LoggerFactoryWrapper;
import org.killbill.billing.payment.retry.DefaultRetryService;
import org.killbill.billing.payment.retry.PaymentRetryNotificationKey;
import org.killbill.billing.util.callcontext.CallContext;
import org.killbill.notificationq.api.NotificationEvent;
import org.killbill.notificationq.api.NotificationEventWithMetadata;
import org.killbill.notificationq.api.NotificationQueue;
import org.killbill.notificationq.api.NotificationQueueService;
import org.killbill.notificationq.api.NotificationQueueService.NoSuchNotificationQueue;
import org.slf4j.Logger;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.List;
import java.util.UUID;


public class PaymentCanceller {

    private final Logger log;
    private final NotificationQueueService notificationQueueService;
    private ProcessorBase processorBase;


    @Inject
    public PaymentCanceller(final NotificationQueueService notificationQueueService, final ProcessorBase processorBase,
                            final LoggerFactoryWrapper loggerFactoryWrapper) {
        this.notificationQueueService = notificationQueueService;
        this.processorBase = processorBase;
        this.log = loggerFactoryWrapper.getLogger(PaymentCanceller.class);
    }


    public void cancelScheduledPaymentTransaction(@Nullable final UUID paymentTransactionId, @Nullable final String paymentTransactionExternalKey, final CallContext callContext) throws PaymentApiException {

        final InternalCallContext internalCallContextWithoutAccountRecordId = processorBase.getInternalCallContextFactory().createInternalCallContextWithoutAccountRecordId(callContext);
        final String effectivePaymentTransactionExternalKey;
        if (paymentTransactionExternalKey == null) {
            final PaymentTransactionModelDao transaction = processorBase.getPaymentDao().getPaymentTransaction(paymentTransactionId, internalCallContextWithoutAccountRecordId);
            effectivePaymentTransactionExternalKey = transaction.getTransactionExternalKey();
        } else {
            effectivePaymentTransactionExternalKey = paymentTransactionExternalKey;
        }

        final List<PaymentAttemptModelDao> attempts = processorBase.getPaymentDao().getPaymentAttemptByTransactionExternalKey(effectivePaymentTransactionExternalKey, internalCallContextWithoutAccountRecordId);
        if (attempts.isEmpty()) {
            return;
        }

        final PaymentAttemptModelDao lastPaymentAttempt = attempts.get(attempts.size() - 1);
        final InternalCallContext internalCallContext = processorBase.getInternalCallContextFactory().createInternalCallContext(lastPaymentAttempt.getAccountId(), callContext);

        cancelScheduledPaymentTransaction(lastPaymentAttempt.getId(), internalCallContext);
    }

    public void cancelScheduledPaymentTransaction(final UUID lastPaymentAttemptId, final InternalCallContext internalCallContext) throws PaymentApiException {
        try {
            final NotificationQueue retryQueue = notificationQueueService.getNotificationQueue(DefaultPaymentService.SERVICE_NAME, DefaultRetryService.QUEUE_NAME);
            final Iterable<NotificationEventWithMetadata<NotificationEvent>> notificationEventWithMetadatas =
                    retryQueue.getFutureNotificationForSearchKeys(internalCallContext.getAccountRecordId(), internalCallContext.getTenantRecordId());

            for (final NotificationEventWithMetadata<NotificationEvent> notificationEvent : notificationEventWithMetadatas) {
                if (((PaymentRetryNotificationKey) notificationEvent.getEvent()).getAttemptId().equals(lastPaymentAttemptId)) {
                    retryQueue.removeNotification(notificationEvent.getRecordId());
                }
                // Go through all results to close the connection
            }
        } catch (final NoSuchNotificationQueue noSuchNotificationQueue) {
            log.error("ERROR Loading Notification Queue - " + noSuchNotificationQueue.getMessage());
            throw new IllegalStateException(noSuchNotificationQueue);
        }
    }
}

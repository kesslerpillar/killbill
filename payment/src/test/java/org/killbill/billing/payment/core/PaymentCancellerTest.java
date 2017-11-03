package org.killbill.billing.payment.core;

import org.killbill.billing.callcontext.InternalCallContext;
import org.killbill.billing.payment.dao.PaymentAttemptModelDao;
import org.killbill.billing.payment.dao.PaymentDao;
import org.killbill.billing.payment.dao.PaymentTransactionModelDao;
import org.killbill.billing.payment.glue.DefaultPaymentService;
import org.killbill.billing.payment.logging.LoggerFactoryWrapper;
import org.killbill.billing.payment.retry.DefaultRetryService;
import org.killbill.billing.payment.retry.PaymentRetryNotificationKey;
import org.killbill.billing.util.callcontext.CallContext;
import org.killbill.billing.util.callcontext.InternalCallContextFactory;
import org.killbill.bus.api.BusEventWithMetadata;
import org.killbill.notificationq.api.NotificationEvent;
import org.killbill.notificationq.api.NotificationEventWithMetadata;
import org.killbill.notificationq.api.NotificationQueue;
import org.killbill.notificationq.api.NotificationQueueService;
import org.slf4j.Logger;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.mockito.Mockito.*;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.FileAssert.fail;


public class PaymentCancellerTest {

    public static final String TEST_TRANSACTION_EXTERNAL_KEY = "testTransactionExternalKey";
    public static final long EXPECTED_ACCOUNT_RECORD_ID = 6L;
    public static final long EXPECTED_TENANT_RECORD_ID = 7L;
    private PaymentCanceller paymentCanceller;
    private NotificationQueueService mockNotificationQueueService;
    private ProcessorBase mockProcessorBase;
    private InternalCallContextFactory mockInternalCallContextFactory;
    private InternalCallContext mockInternalCallContextWithoutAccountRecordId;
    private InternalCallContext mockInternalCallContext;
    private PaymentDao mockPaymentDao;
    private UUID uuid;
    private CallContext mockCallContext;
    private PaymentTransactionModelDao mockPaymentTransactionModelDao;
    private PaymentAttemptModelDao mockPaymentAttemptModelDao;
    private NotificationQueue mockNotificationQueue;
    private List<NotificationEventWithMetadata<NotificationEvent>> futureNotificationForSearchKeys;
    private LoggerFactoryWrapper mockLoggerFactoryWrapper;
    private Logger mockLogger;

    @BeforeMethod
    public void setUp() throws Exception {
        uuid = new UUID(1L, 1L);
        futureNotificationForSearchKeys = new ArrayList<NotificationEventWithMetadata<NotificationEvent>>();
        mockNotificationQueueService = mock(NotificationQueueService.class);
        mockNotificationQueue = mock(NotificationQueue.class);
        mockProcessorBase = mock(ProcessorBase.class);
        mockInternalCallContextFactory = mock(InternalCallContextFactory.class);
        mockCallContext = mock(CallContext.class);
        mockInternalCallContextWithoutAccountRecordId = mock(InternalCallContext.class);
        mockInternalCallContext = mock(InternalCallContext.class);
        mockPaymentDao = mock(PaymentDao.class);
        mockPaymentTransactionModelDao = mock(PaymentTransactionModelDao.class);
        mockPaymentAttemptModelDao = mock(PaymentAttemptModelDao.class);
        mockLoggerFactoryWrapper = mock(LoggerFactoryWrapper.class);
        mockLogger = mock(Logger.class);

        when(mockProcessorBase.getInternalCallContextFactory()).thenReturn(mockInternalCallContextFactory);
        when(mockProcessorBase.getPaymentDao()).thenReturn(mockPaymentDao);
        when(mockInternalCallContextFactory.createInternalCallContextWithoutAccountRecordId(mockCallContext))
                .thenReturn(mockInternalCallContextWithoutAccountRecordId);
        when(mockPaymentDao.getPaymentTransaction(uuid, mockInternalCallContextWithoutAccountRecordId))
                .thenReturn(mockPaymentTransactionModelDao);
        when(mockPaymentTransactionModelDao.getTransactionExternalKey()).thenReturn(TEST_TRANSACTION_EXTERNAL_KEY);
        when(mockNotificationQueueService
                .getNotificationQueue(DefaultPaymentService.SERVICE_NAME, DefaultRetryService.QUEUE_NAME))
                .thenReturn(mockNotificationQueue);
        when(mockInternalCallContext.getAccountRecordId()).thenReturn(EXPECTED_ACCOUNT_RECORD_ID);
        when(mockInternalCallContext.getTenantRecordId()).thenReturn(EXPECTED_TENANT_RECORD_ID);
        when(mockNotificationQueue
                .getFutureNotificationForSearchKeys(EXPECTED_ACCOUNT_RECORD_ID, EXPECTED_TENANT_RECORD_ID))
                .thenReturn(futureNotificationForSearchKeys);
        when(mockLoggerFactoryWrapper.getLogger(PaymentCanceller.class)).thenReturn(mockLogger);

        paymentCanceller = new PaymentCanceller(mockNotificationQueueService, mockProcessorBase,
                mockLoggerFactoryWrapper);
    }

    @Test
    public void testCancelScheduledPaymentTransaction_PaymentTransactionExternalKey_Null_AndAttemptsIsEmpty() throws Exception {
        List<PaymentAttemptModelDao> paymentAttemptModelDaoArrayList = new ArrayList<PaymentAttemptModelDao>();

        when(mockPaymentDao
                .getPaymentAttemptByTransactionExternalKey(TEST_TRANSACTION_EXTERNAL_KEY, mockInternalCallContextWithoutAccountRecordId))
                .thenReturn(paymentAttemptModelDaoArrayList);

        paymentCanceller.cancelScheduledPaymentTransaction(uuid, null, mockCallContext);

        verify(mockInternalCallContextFactory, times(0))
                .createInternalCallContext(any(UUID.class), any(CallContext.class));
        verify(mockNotificationQueueService, times(0))
                .getNotificationQueue(DefaultPaymentService.SERVICE_NAME, DefaultRetryService.QUEUE_NAME);
    }

    @Test
    public void testCancelScheduledPaymentTransaction_PaymentTransactionExternalKey_NotNull() throws Exception {
        String paymentTransactionExternalKey = "test";
        long expectedAccountRecordId = 4L;
        long expecteTenantRecordId = 5L;
        UUID expectedId = new UUID(2L, 2L);
        UUID expectedAccountId = new UUID(3L, 3L);

        List<PaymentAttemptModelDao> paymentAttemptModelDaoArrayList = new ArrayList<PaymentAttemptModelDao>();
        paymentAttemptModelDaoArrayList.add(null);
        paymentAttemptModelDaoArrayList.add(mockPaymentAttemptModelDao);

        when(mockPaymentDao
                .getPaymentAttemptByTransactionExternalKey(paymentTransactionExternalKey, mockInternalCallContextWithoutAccountRecordId))
                .thenReturn(paymentAttemptModelDaoArrayList);
        when(mockPaymentAttemptModelDao.getAccountId()).thenReturn(expectedAccountId);
        when(mockPaymentAttemptModelDao.getId()).thenReturn(expectedId);
        when(mockInternalCallContextFactory.createInternalCallContext(expectedAccountId, mockCallContext))
                .thenReturn(mockInternalCallContext);

        when(mockInternalCallContext.getAccountRecordId()).thenReturn(expectedAccountRecordId);
        when(mockInternalCallContext.getTenantRecordId()).thenReturn(expecteTenantRecordId);

        when(mockNotificationQueue
                .getFutureNotificationForSearchKeys(expectedAccountRecordId, expecteTenantRecordId))
                .thenReturn(futureNotificationForSearchKeys);

        paymentCanceller.cancelScheduledPaymentTransaction(uuid, paymentTransactionExternalKey, mockCallContext);

        verify(mockNotificationQueueService, times(1))
                .getNotificationQueue(DefaultPaymentService.SERVICE_NAME, DefaultRetryService.QUEUE_NAME);
    }

    @Test
    public void testCancelScheduledPaymentTransaction_LastPaymentId_NotEqual() throws Exception {
        NotificationEventWithMetadata<NotificationEvent> mockNotificationEventWithMetadata = mock(NotificationEventWithMetadata.class);
        PaymentRetryNotificationKey mockNotificationEvent = mock(PaymentRetryNotificationKey.class);
        when(mockNotificationEventWithMetadata.getEvent()).thenReturn(mockNotificationEvent);
        when(mockNotificationEvent.getAttemptId()).thenReturn(new UUID(10L, 10L));
        futureNotificationForSearchKeys.add(mockNotificationEventWithMetadata);

        paymentCanceller.cancelScheduledPaymentTransaction(uuid, mockInternalCallContext);

        verify(mockNotificationQueue, times(0)).removeNotification(anyLong());
    }

    @Test
    public void testCancelScheduledPaymentTransaction_LastPaymentId_Equal() throws Exception {
        long expectedRecordId = 11L;
        NotificationEventWithMetadata<NotificationEvent> mockNotificationEventWithMetadata = mock(NotificationEventWithMetadata.class);
        PaymentRetryNotificationKey mockPaymentRetryNotificationKey = mock(PaymentRetryNotificationKey.class);

        when(mockNotificationEventWithMetadata.getEvent()).thenReturn(mockPaymentRetryNotificationKey);
        when(mockPaymentRetryNotificationKey.getAttemptId()).thenReturn(uuid);
        when(mockNotificationEventWithMetadata.getRecordId()).thenReturn(expectedRecordId);

        futureNotificationForSearchKeys.add(mockNotificationEventWithMetadata);

        paymentCanceller.cancelScheduledPaymentTransaction(uuid, mockInternalCallContext);

        verify(mockNotificationQueue).removeNotification(expectedRecordId);
    }

    @Test
    public void testCancelScheduledPaymentTransaction_ThrowsException() throws Exception {
        long expectedRecordId = 11L;
        NotificationQueueService.NoSuchNotificationQueue expectedException = new NotificationQueueService.NoSuchNotificationQueue("test");
        NotificationEventWithMetadata<NotificationEvent> mockNotificationEventWithMetadata = mock(NotificationEventWithMetadata.class);
        PaymentRetryNotificationKey mockPaymentRetryNotificationKey = mock(PaymentRetryNotificationKey.class);

        when(mockNotificationQueueService
                .getNotificationQueue(DefaultPaymentService.SERVICE_NAME, DefaultRetryService.QUEUE_NAME))
                .thenThrow(expectedException);

        futureNotificationForSearchKeys.add(mockNotificationEventWithMetadata);

        try {
            paymentCanceller.cancelScheduledPaymentTransaction(uuid, mockInternalCallContext);
            fail();
        }catch (Exception e){
            assertTrue(e instanceof IllegalStateException);
            assertSame(expectedException, e.getCause());
            verify(mockNotificationQueue, times(0)).removeNotification(expectedRecordId);
            verify(mockLogger).error("ERROR Loading Notification Queue - test");
        }
    }

}
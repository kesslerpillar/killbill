package org.killbill.billing.payment.core;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.*;
import org.killbill.billing.ErrorCode;
import org.killbill.billing.callcontext.InternalTenantContext;
import org.killbill.billing.payment.api.*;
import org.killbill.billing.payment.core.janitor.IncompletePaymentTransactionTask;
import org.killbill.billing.payment.dao.PaymentAttemptModelDao;
import org.killbill.billing.payment.dao.PaymentModelDao;
import org.killbill.billing.payment.dao.PaymentTransactionModelDao;
import org.killbill.billing.payment.dao.PluginPropertySerializer;
import org.killbill.billing.payment.glue.DefaultPaymentService;
import org.killbill.billing.payment.plugin.api.PaymentPluginApi;
import org.killbill.billing.payment.plugin.api.PaymentPluginApiException;
import org.killbill.billing.payment.plugin.api.PaymentTransactionInfoPlugin;
import org.killbill.billing.payment.retry.DefaultRetryService;
import org.killbill.billing.payment.retry.PaymentRetryNotificationKey;
import org.killbill.billing.util.callcontext.TenantContext;
import org.killbill.billing.util.entity.DefaultPagination;
import org.killbill.billing.util.entity.Pagination;
import org.killbill.billing.util.entity.dao.DefaultPaginationHelper;
import org.killbill.notificationq.api.NotificationEvent;
import org.killbill.notificationq.api.NotificationEventWithMetadata;
import org.killbill.notificationq.api.NotificationQueue;
import org.killbill.notificationq.api.NotificationQueueService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.*;

import static org.killbill.billing.util.entity.dao.DefaultPaginationHelper.getEntityPagination;
import static org.killbill.billing.util.entity.dao.DefaultPaginationHelper.getEntityPaginationFromPlugins;

public class PaymentLocator {
    private static final String SCHEDULED = "SCHEDULED";
    private static final ImmutableList<PluginProperty> PLUGIN_PROPERTIES = ImmutableList.of();
    private static final Logger log = LoggerFactory.getLogger(PaymentLocator.class);

    private final ProcessorBase processorBase;
    private final IncompletePaymentTransactionTask incompletePaymentTransactionTask;
    private final NotificationQueueService notificationQueueService;

    @Inject
    public PaymentLocator(final IncompletePaymentTransactionTask incompletePaymentTransactionTask,
                          final NotificationQueueService notificationQueueService,
                          final ProcessorBase processorBase) {
        this.incompletePaymentTransactionTask = incompletePaymentTransactionTask;
        this.notificationQueueService = notificationQueueService;
        this.processorBase = processorBase;
    }


    public List<Payment> getAccountPayments(final UUID accountId, final boolean withPluginInfo, final boolean withAttempts, final TenantContext context, final InternalTenantContext tenantContext) throws PaymentApiException {
        final List<PaymentModelDao> paymentsModelDao = processorBase.getPaymentDao().getPaymentsForAccount(accountId, tenantContext);
        final List<PaymentTransactionModelDao> transactionsModelDao = processorBase.getPaymentDao().getTransactionsForAccount(accountId, tenantContext);

        final Map<UUID, PaymentPluginApi> paymentPluginByPaymentMethodId = new HashMap<UUID, PaymentPluginApi>();
        final Collection<UUID> absentPlugins = new HashSet<UUID>();
        final List<Payment> transformedPayments = Lists.transform(paymentsModelDao,
                new Function<PaymentModelDao, Payment>() {
                    @Override
                    public Payment apply(final PaymentModelDao paymentModelDao) {
                        List<PaymentTransactionInfoPlugin> pluginInfo = null;

                        if (withPluginInfo) {
                            PaymentPluginApi pluginApi = paymentPluginByPaymentMethodId.get(paymentModelDao.getPaymentMethodId());
                            if (pluginApi == null && !absentPlugins.contains(paymentModelDao.getPaymentMethodId())) {
                                try {
                                    pluginApi = processorBase.getPaymentProviderPlugin(paymentModelDao.getPaymentMethodId(), true, tenantContext);
                                    paymentPluginByPaymentMethodId.put(paymentModelDao.getPaymentMethodId(), pluginApi);
                                } catch (final PaymentApiException e) {
                                    log.warn("Unable to retrieve pluginApi for payment method " + paymentModelDao.getPaymentMethodId());
                                    absentPlugins.add(paymentModelDao.getPaymentMethodId());
                                }
                            }

                            pluginInfo = getPaymentTransactionInfoPluginsIfNeeded(pluginApi, paymentModelDao, context);
                        }

                        return toPayment(paymentModelDao, transactionsModelDao, pluginInfo, withAttempts, tenantContext);
                    }
                });

        // Copy the transformed list, so the transformation function is applied once (otherwise, the Janitor could be invoked multiple times)
        return ImmutableList.copyOf(transformedPayments);
    }

    public Payment getPayment(final UUID paymentId, final boolean withPluginInfo, final boolean withAttempts, final Iterable<PluginProperty> properties, final TenantContext tenantContext, final InternalTenantContext internalTenantContext) throws PaymentApiException {
        final PaymentModelDao paymentModelDao = processorBase.getPaymentDao().getPayment(paymentId, internalTenantContext);
        return getPayment(paymentModelDao, withPluginInfo, withAttempts, properties, tenantContext, internalTenantContext);
    }

    public Payment getPaymentByExternalKey(final String paymentExternalKey, final boolean withPluginInfo, final boolean withAttempts, final Iterable<PluginProperty> properties, final TenantContext tenantContext, final InternalTenantContext internalTenantContext) throws PaymentApiException {
        final PaymentModelDao paymentModelDao = processorBase.getPaymentDao().getPaymentByExternalKey(paymentExternalKey, internalTenantContext);
        return getPayment(paymentModelDao, withPluginInfo, withAttempts, properties, tenantContext, internalTenantContext);
    }

    public Payment getPayment(final PaymentModelDao paymentModelDao, final boolean withPluginInfo, final boolean withAttempts, final Iterable<PluginProperty> properties, final TenantContext tenantContext, final InternalTenantContext internalTenantContext) throws PaymentApiException {
        if (paymentModelDao == null) {
            return null;
        }
        return toPayment(paymentModelDao, withPluginInfo, withAttempts, properties, tenantContext, internalTenantContext);
    }

    public Pagination<Payment> getPayments(final Long offset, final Long limit, final String pluginName, final boolean withPluginInfo, final boolean withAttempts, final TenantContext tenantContext, final InternalTenantContext internalTenantContext) throws PaymentApiException {
        final PaymentPluginApi pluginApi = withPluginInfo ? processorBase.getPaymentPluginApi(pluginName) : null;

        return getEntityPagination(limit,
                new DefaultPaginationHelper.SourcePaginationBuilder<PaymentModelDao, PaymentApiException>() {
                    @Override
                    public Pagination<PaymentModelDao> build() {
                        // Find all payments for all accounts
                        return processorBase.getPaymentDao().getPayments(pluginName, offset, limit, internalTenantContext);
                    }
                },
                new Function<PaymentModelDao, Payment>() {
                    @Override
                    public Payment apply(final PaymentModelDao paymentModelDao) {
                        final List<PaymentTransactionInfoPlugin> pluginInfo = getPaymentTransactionInfoPluginsIfNeeded(pluginApi, paymentModelDao, tenantContext);
                        return toPayment(paymentModelDao.getId(), pluginInfo, withAttempts, internalTenantContext);
                    }
                }
        );
    }

    public Pagination<Payment> getPayments(final Long offset, final Long limit, final boolean withPluginInfo, final boolean withAttempts,
                                           final TenantContext tenantContext, final InternalTenantContext internalTenantContext) {
        final Map<UUID, Optional<PaymentPluginApi>> paymentMethodIdToPaymentPluginApi = new HashMap<UUID, Optional<PaymentPluginApi>>();

        try {
            return getEntityPagination(limit,
                    new DefaultPaginationHelper.SourcePaginationBuilder<PaymentModelDao, PaymentApiException>() {
                        @Override
                        public Pagination<PaymentModelDao> build() {
                            // Find all payments for all accounts
                            return processorBase.getPaymentDao().get(offset, limit, internalTenantContext);
                        }
                    },
                    new Function<PaymentModelDao, Payment>() {
                        @Override
                        public Payment apply(final PaymentModelDao paymentModelDao) {
                            final PaymentPluginApi pluginApi;
                            if (!withPluginInfo) {
                                pluginApi = null;
                            } else {
                                if (paymentMethodIdToPaymentPluginApi.get(paymentModelDao.getPaymentMethodId()) == null) {
                                    try {
                                        final PaymentPluginApi paymentProviderPlugin = processorBase.getPaymentProviderPlugin(paymentModelDao.getPaymentMethodId(), true, internalTenantContext);
                                        paymentMethodIdToPaymentPluginApi.put(paymentModelDao.getPaymentMethodId(), Optional.of(paymentProviderPlugin));
                                    } catch (final PaymentApiException e) {
                                        log.warn("Unable to retrieve PaymentPluginApi for paymentMethodId='{}'", paymentModelDao.getPaymentMethodId(), e);
                                        // We use Optional to avoid printing the log line for each result
                                        paymentMethodIdToPaymentPluginApi.put(paymentModelDao.getPaymentMethodId(), Optional.<PaymentPluginApi>absent());
                                    }
                                }
                                pluginApi = paymentMethodIdToPaymentPluginApi.get(paymentModelDao.getPaymentMethodId()).orNull();
                            }
                            final List<PaymentTransactionInfoPlugin> pluginInfo = getPaymentTransactionInfoPluginsIfNeeded(pluginApi, paymentModelDao, tenantContext);
                            return toPayment(paymentModelDao.getId(), pluginInfo, withAttempts, internalTenantContext);
                        }
                    }
            );
        } catch (final PaymentApiException e) {
            log.warn("Unable to get payments", e);
            return new DefaultPagination<Payment>(offset, limit, null, null, ImmutableSet.<Payment>of().iterator());
        }
    }


    public Pagination<Payment> searchPayments(final String searchKey, final Long offset, final Long limit, final boolean withPluginInfo, final boolean withAttempts, final Iterable<PluginProperty> properties, final TenantContext tenantContext, final InternalTenantContext internalTenantContext) {
        if (withPluginInfo) {
            return getEntityPaginationFromPlugins(false,
                    processorBase.getAvailablePlugins(),
                    offset,
                    limit,
                    new DefaultPaginationHelper.EntityPaginationBuilder<Payment, PaymentApiException>() {
                        @Override
                        public Pagination<Payment> build(final Long offset, final Long limit, final String pluginName) throws PaymentApiException {
                            return searchPayments(searchKey, offset, limit, pluginName, withPluginInfo, withAttempts, properties, tenantContext, internalTenantContext);
                        }
                    }
            );
        } else {
            try {
                return getEntityPagination(limit,
                        new DefaultPaginationHelper.SourcePaginationBuilder<PaymentModelDao, PaymentApiException>() {
                            @Override
                            public Pagination<PaymentModelDao> build() {
                                return processorBase.getPaymentDao().searchPayments(searchKey, offset, limit, internalTenantContext);
                            }
                        },
                        new Function<PaymentModelDao, Payment>() {
                            @Override
                            public Payment apply(final PaymentModelDao paymentModelDao) {
                                return toPayment(paymentModelDao.getId(), null, withAttempts, internalTenantContext);
                            }
                        }
                );
            } catch (final PaymentApiException e) {
                log.warn("Unable to search through payments", e);
                return new DefaultPagination<Payment>(offset, limit, null, null, ImmutableSet.<Payment>of().iterator());
            }
        }
    }

    public Pagination<Payment> searchPayments(final String searchKey, final Long offset, final Long limit, final String pluginName, final boolean withPluginInfo,
                                              final boolean withAttempts, final Iterable<PluginProperty> properties, final TenantContext tenantContext, final InternalTenantContext internalTenantContext) throws PaymentApiException {
        final PaymentPluginApi pluginApi = processorBase.getPaymentPluginApi(pluginName);

        return getEntityPagination(limit,
                new DefaultPaginationHelper.SourcePaginationBuilder<PaymentTransactionInfoPlugin, PaymentApiException>() {
                    @Override
                    public Pagination<PaymentTransactionInfoPlugin> build() throws PaymentApiException {
                        try {
                            return pluginApi.searchPayments(searchKey, offset, limit, properties, tenantContext);
                        } catch (final PaymentPluginApiException e) {
                            throw new PaymentApiException(e, ErrorCode.PAYMENT_PLUGIN_SEARCH_PAYMENTS, pluginName, searchKey);
                        }
                    }

                },
                new Function<PaymentTransactionInfoPlugin, Payment>() {
                    final List<PaymentTransactionInfoPlugin> cachedPaymentTransactions = new LinkedList<PaymentTransactionInfoPlugin>();

                    @Override
                    public Payment apply(final PaymentTransactionInfoPlugin pluginTransaction) {
                        if (pluginTransaction.getKbPaymentId() == null) {
                            // Garbage from the plugin?
                            log.debug("Plugin {} returned a payment without a kbPaymentId for searchKey {}", pluginName, searchKey);
                            return null;
                        }

                        if (cachedPaymentTransactions.isEmpty() ||
                                (cachedPaymentTransactions.get(0).getKbPaymentId().equals(pluginTransaction.getKbPaymentId()))) {
                            cachedPaymentTransactions.add(pluginTransaction);
                            return null;
                        } else {
                            final Payment result = toPayment(pluginTransaction.getKbPaymentId(), withPluginInfo ? ImmutableList.copyOf(cachedPaymentTransactions) : ImmutableList.<PaymentTransactionInfoPlugin>of(), withAttempts, internalTenantContext);
                            cachedPaymentTransactions.clear();
                            cachedPaymentTransactions.add(pluginTransaction);
                            return result;
                        }
                    }
                }
        );
    }


    public Payment getPaymentByTransactionId(final UUID transactionId, final boolean withPluginInfo, final boolean withAttempts, final Iterable<PluginProperty> properties, final TenantContext tenantContext, final InternalTenantContext internalTenantContext) throws PaymentApiException {
        final PaymentTransactionModelDao paymentTransactionDao = processorBase.getPaymentDao().getPaymentTransaction(transactionId, internalTenantContext);
        if (null != paymentTransactionDao) {
            PaymentModelDao paymentModelDao = processorBase.getPaymentDao().getPayment(paymentTransactionDao.getPaymentId(), internalTenantContext);
            return toPayment(paymentModelDao, withPluginInfo, withAttempts, properties, tenantContext, internalTenantContext);
        }
        return null;
    }

    // Used in bulk get APIs (getPayments / searchPayments)
    private Payment toPayment(final UUID paymentId, @Nullable final Iterable<PaymentTransactionInfoPlugin> pluginTransactions, final boolean withAttempts, final InternalTenantContext tenantContext) {
        final PaymentModelDao paymentModelDao = processorBase.getPaymentDao().getPayment(paymentId, tenantContext);
        if (paymentModelDao == null) {
            log.warn("Unable to find payment id " + paymentId);
            return null;
        }

        return toPayment(paymentModelDao, pluginTransactions, withAttempts, tenantContext);
    }

    private Payment toPayment(final PaymentModelDao paymentModelDao, @Nullable final Iterable<PaymentTransactionInfoPlugin> pluginTransactions,
                              final boolean withAttempts, final InternalTenantContext tenantContext) {
        final InternalTenantContext tenantContextWithAccountRecordId = getInternalTenantContextWithAccountRecordId(paymentModelDao.getAccountId(), tenantContext);
        final List<PaymentTransactionModelDao> transactionsForPayment = processorBase.getPaymentDao().getTransactionsForPayment(paymentModelDao.getId(), tenantContextWithAccountRecordId);

        return toPayment(paymentModelDao, transactionsForPayment, pluginTransactions, withAttempts, tenantContextWithAccountRecordId);
    }

    // Used in bulk get API (getAccountPayments)
    private Payment toPayment(final PaymentModelDao curPaymentModelDao, final Collection<PaymentTransactionModelDao> curTransactionsModelDao, @Nullable final Iterable<PaymentTransactionInfoPlugin> pluginTransactions, final boolean withAttempts, final InternalTenantContext internalTenantContext) {
        final Collection<PaymentTransactionModelDao> transactionsModelDao = new LinkedList<PaymentTransactionModelDao>(curTransactionsModelDao);
        invokeJanitor(curPaymentModelDao, transactionsModelDao, pluginTransactions, internalTenantContext);

        final Collection<PaymentTransaction> transactions = new LinkedList<PaymentTransaction>();
        for (final PaymentTransactionModelDao newPaymentTransactionModelDao : transactionsModelDao) {
            final PaymentTransactionInfoPlugin paymentTransactionInfoPlugin = findPaymentTransactionInfoPlugin(newPaymentTransactionModelDao, pluginTransactions);
            final PaymentTransaction transaction = new DefaultPaymentTransaction(newPaymentTransactionModelDao.getId(),
                    newPaymentTransactionModelDao.getAttemptId(),
                    newPaymentTransactionModelDao.getTransactionExternalKey(),
                    newPaymentTransactionModelDao.getCreatedDate(),
                    newPaymentTransactionModelDao.getUpdatedDate(),
                    newPaymentTransactionModelDao.getPaymentId(),
                    newPaymentTransactionModelDao.getTransactionType(),
                    newPaymentTransactionModelDao.getEffectiveDate(),
                    newPaymentTransactionModelDao.getTransactionStatus(),
                    newPaymentTransactionModelDao.getAmount(),
                    newPaymentTransactionModelDao.getCurrency(),
                    newPaymentTransactionModelDao.getProcessedAmount(),
                    newPaymentTransactionModelDao.getProcessedCurrency(),
                    newPaymentTransactionModelDao.getGatewayErrorCode(),
                    newPaymentTransactionModelDao.getGatewayErrorMsg(),
                    paymentTransactionInfoPlugin);
            transactions.add(transaction);
        }

        final Ordering<PaymentTransaction> perPaymentTransactionOrdering = Ordering.from(new Comparator<PaymentTransaction>() {
            @Override
            public int compare(final PaymentTransaction o1, final PaymentTransaction o2) {
                return o1.getEffectiveDate().compareTo(o2.getEffectiveDate());
            }
        });
        final List<PaymentTransaction> sortedTransactions = perPaymentTransactionOrdering.immutableSortedCopy(transactions);
        return new DefaultPayment(curPaymentModelDao.getId(),
                curPaymentModelDao.getCreatedDate(),
                curPaymentModelDao.getUpdatedDate(),
                curPaymentModelDao.getAccountId(),
                curPaymentModelDao.getPaymentMethodId(),
                curPaymentModelDao.getPaymentNumber(),
                curPaymentModelDao.getExternalKey(),
                sortedTransactions,
                (withAttempts && !sortedTransactions.isEmpty()) ?
                        getPaymentAttempts(processorBase.getPaymentDao().getPaymentAttempts(curPaymentModelDao.getExternalKey(), internalTenantContext),
                                internalTenantContext) : null
        );
    }



    private InternalTenantContext getInternalTenantContextWithAccountRecordId(final UUID accountId, final InternalTenantContext tenantContext) {
        final InternalTenantContext tenantContextWithAccountRecordId;
        if (tenantContext.getAccountRecordId() == null) {
            tenantContextWithAccountRecordId = processorBase.getInternalCallContextFactory().createInternalTenantContext(accountId, tenantContext);
        } else {
            tenantContextWithAccountRecordId = tenantContext;
        }
        return tenantContextWithAccountRecordId;
    }

    // Used in bulk get API (getAccountPayments / getPayments)
    private List<PaymentTransactionInfoPlugin> getPaymentTransactionInfoPluginsIfNeeded(@Nullable final PaymentPluginApi pluginApi, final PaymentModelDao paymentModelDao, final TenantContext context) {
        if (pluginApi == null) {
            return null;
        }

        try {
            return getPaymentTransactionInfoPlugins(pluginApi, paymentModelDao, PLUGIN_PROPERTIES, context);
        } catch (final PaymentApiException e) {
            log.warn("Unable to retrieve plugin info for payment " + paymentModelDao.getId());
            return null;
        }
    }

    private List<PaymentTransactionInfoPlugin> getPaymentTransactionInfoPlugins(final PaymentPluginApi plugin, final PaymentModelDao paymentModelDao, final Iterable<PluginProperty> properties, final TenantContext context) throws PaymentApiException {
        try {
            return plugin.getPaymentInfo(paymentModelDao.getAccountId(), paymentModelDao.getId(), properties, context);
        } catch (final PaymentPluginApiException e) {
            throw new PaymentApiException(e, ErrorCode.PAYMENT_PLUGIN_GET_PAYMENT_INFO, paymentModelDao.getId(), e.toString());
        }
    }

    public PaymentModelDao invokeJanitor(final PaymentModelDao curPaymentModelDao, final Collection<PaymentTransactionModelDao> curTransactionsModelDao, @Nullable final Iterable<PaymentTransactionInfoPlugin> pluginTransactions, final InternalTenantContext internalTenantContext) {
        // Need to filter for optimized codepaths looking up by account_record_id
        final Iterable<PaymentTransactionModelDao> filteredTransactions = Iterables.filter(curTransactionsModelDao, new Predicate<PaymentTransactionModelDao>() {
            @Override
            public boolean apply(final PaymentTransactionModelDao curPaymentTransactionModelDao) {
                return curPaymentTransactionModelDao.getPaymentId().equals(curPaymentModelDao.getId());
            }
        });

        PaymentModelDao newPaymentModelDao = curPaymentModelDao;
        final Collection<PaymentTransactionModelDao> transactionsModelDao = new LinkedList<PaymentTransactionModelDao>();
        for (final PaymentTransactionModelDao curPaymentTransactionModelDao : filteredTransactions) {
            PaymentTransactionModelDao newPaymentTransactionModelDao = curPaymentTransactionModelDao;

            final PaymentTransactionInfoPlugin paymentTransactionInfoPlugin = findPaymentTransactionInfoPlugin(newPaymentTransactionModelDao, pluginTransactions);
            if (paymentTransactionInfoPlugin != null) {
                // Make sure to invoke the Janitor task in case the plugin fixes its state on the fly
                // See https://github.com/killbill/killbill/issues/341
                final boolean hasChanged = incompletePaymentTransactionTask.updatePaymentAndTransactionIfNeededWithAccountLock(newPaymentModelDao, newPaymentTransactionModelDao, paymentTransactionInfoPlugin, internalTenantContext);
                if (hasChanged) {
                    newPaymentModelDao = processorBase.getPaymentDao().getPayment(newPaymentModelDao.getId(), internalTenantContext);
                    newPaymentTransactionModelDao = processorBase.getPaymentDao().getPaymentTransaction(newPaymentTransactionModelDao.getId(), internalTenantContext);
                }
            }

            transactionsModelDao.add(newPaymentTransactionModelDao);
        }

        curTransactionsModelDao.clear();
        curTransactionsModelDao.addAll(transactionsModelDao);

        return newPaymentModelDao;
    }

    private List<PaymentAttempt> getPaymentAttempts(final List<PaymentAttemptModelDao> pastPaymentAttempts,
                                                    final InternalTenantContext internalTenantContext) {

        List<PaymentAttempt> paymentAttempts = new ArrayList<PaymentAttempt>();

        // Add Past Payment Attempts
        for (PaymentAttemptModelDao pastPaymentAttempt : pastPaymentAttempts) {
            DefaultPaymentAttempt paymentAttempt = new DefaultPaymentAttempt(
                    pastPaymentAttempt.getAccountId(),
                    pastPaymentAttempt.getPaymentMethodId(),
                    pastPaymentAttempt.getId(),
                    pastPaymentAttempt.getCreatedDate(),
                    pastPaymentAttempt.getUpdatedDate(),
                    pastPaymentAttempt.getCreatedDate(),
                    pastPaymentAttempt.getPaymentExternalKey(),
                    pastPaymentAttempt.getTransactionId(),
                    pastPaymentAttempt.getTransactionExternalKey(),
                    pastPaymentAttempt.getTransactionType(),
                    pastPaymentAttempt.getStateName(),
                    pastPaymentAttempt.getAmount(),
                    pastPaymentAttempt.getCurrency(),
                    pastPaymentAttempt.getPluginName(),
                    buildPluginProperties(pastPaymentAttempt));
            paymentAttempts.add(paymentAttempt);
        }

        // Get Future Payment Attempts from Notification Queue and add them to the list
        try {
            final NotificationQueue retryQueue = notificationQueueService.getNotificationQueue(DefaultPaymentService.SERVICE_NAME, DefaultRetryService.QUEUE_NAME);
            final Iterable<NotificationEventWithMetadata<NotificationEvent>> notificationEventWithMetadatas =
                    retryQueue.getFutureNotificationForSearchKeys(internalTenantContext.getAccountRecordId(), internalTenantContext.getTenantRecordId());

            for (final NotificationEventWithMetadata<NotificationEvent> notificationEvent : notificationEventWithMetadatas) {
                // Last Attempt
                PaymentAttemptModelDao lastPaymentAttempt = getLastPaymentAttempt(pastPaymentAttempts,
                        ((PaymentRetryNotificationKey) notificationEvent.getEvent()).getAttemptId());

                if (lastPaymentAttempt != null) {
                    DefaultPaymentAttempt futurePaymentAttempt = new DefaultPaymentAttempt(
                            lastPaymentAttempt.getAccountId(), // accountId
                            lastPaymentAttempt.getPaymentMethodId(), // paymentMethodId
                            ((PaymentRetryNotificationKey) notificationEvent.getEvent()).getAttemptId(), // id
                            null, // createdDate
                            null, // updatedDate
                            notificationEvent.getEffectiveDate(), // effectiveDate
                            lastPaymentAttempt.getPaymentExternalKey(), // paymentExternalKey
                            null, // transactionId
                            lastPaymentAttempt.getTransactionExternalKey(), // transactionExternalKey
                            lastPaymentAttempt.getTransactionType(), // transactionType
                            SCHEDULED, // stateName
                            lastPaymentAttempt.getAmount(), // amount
                            lastPaymentAttempt.getCurrency(), // currency
                            ((PaymentRetryNotificationKey) notificationEvent.getEvent()).getPaymentControlPluginNames().get(0), // pluginName,
                            buildPluginProperties(lastPaymentAttempt)); // pluginProperties
                    paymentAttempts.add(futurePaymentAttempt);
                }
            }
        } catch (NotificationQueueService.NoSuchNotificationQueue noSuchNotificationQueue) {
            log.error("ERROR Loading Notification Queue - " + noSuchNotificationQueue.getMessage());
        }
        return paymentAttempts;
    }

    private PaymentAttemptModelDao getLastPaymentAttempt(final List<PaymentAttemptModelDao> pastPaymentAttempts, final UUID attemptId) {
        if (!pastPaymentAttempts.isEmpty()) {
            for (int i = pastPaymentAttempts.size() - 1; i >= 0; i--) {
                if (pastPaymentAttempts.get(i).getId().equals(attemptId)) {
                    return pastPaymentAttempts.get(i);
                }
            }
        }
        return null;
    }

    private List<PluginProperty> buildPluginProperties(final PaymentAttemptModelDao pastPaymentAttempt) {
        if (pastPaymentAttempt.getPluginProperties() != null) {
            try {
                return Lists.newArrayList(PluginPropertySerializer.deserialize(pastPaymentAttempt.getPluginProperties()));
            } catch (PluginPropertySerializer.PluginPropertySerializerException e) {
                log.error("ERROR Deserializing Plugin Properties - " + e.getMessage());
            }
        }
        return null;
    }

    private PaymentTransactionInfoPlugin findPaymentTransactionInfoPlugin(final PaymentTransactionModelDao paymentTransactionModelDao, @Nullable final Iterable<PaymentTransactionInfoPlugin> pluginTransactions) {
        if (pluginTransactions == null) {
            return null;
        }

        return Iterables.tryFind(pluginTransactions,
                new Predicate<PaymentTransactionInfoPlugin>() {
                    @Override
                    public boolean apply(final PaymentTransactionInfoPlugin paymentTransactionInfoPlugin) {
                        return paymentTransactionModelDao.getId().equals(paymentTransactionInfoPlugin.getKbTransactionPaymentId());
                    }
                }).orNull();
    }

    // Used in single get APIs (getPayment / getPaymentByExternalKey)
    private Payment toPayment(final PaymentModelDao paymentModelDao, final boolean withPluginInfo, final boolean withAttempts, final Iterable<PluginProperty> properties, final TenantContext context, final InternalTenantContext tenantContext) throws PaymentApiException {
        final PaymentPluginApi plugin = processorBase.getPaymentProviderPlugin(paymentModelDao.getPaymentMethodId(), true, tenantContext);
        final List<PaymentTransactionInfoPlugin> pluginTransactions = withPluginInfo ? getPaymentTransactionInfoPlugins(plugin, paymentModelDao, properties, context) : null;

        return toPayment(paymentModelDao, pluginTransactions, withAttempts, tenantContext);
    }

}

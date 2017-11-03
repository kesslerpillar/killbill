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

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.killbill.automaton.OperationResult;
import org.killbill.billing.ErrorCode;
import org.killbill.billing.account.api.Account;
import org.killbill.billing.account.api.AccountApiException;
import org.killbill.billing.callcontext.InternalCallContext;
import org.killbill.billing.catalog.api.Currency;
import org.killbill.billing.payment.api.*;
import org.killbill.billing.payment.core.sm.PaymentAutomatonDAOHelper;
import org.killbill.billing.payment.core.sm.PaymentAutomatonRunner;
import org.killbill.billing.payment.core.sm.PaymentStateContext;
import org.killbill.billing.payment.dao.PaymentModelDao;
import org.killbill.billing.payment.dao.PaymentTransactionModelDao;
import org.killbill.billing.payment.plugin.api.PaymentPluginApi;
import org.killbill.billing.payment.plugin.api.PaymentPluginApiException;
import org.killbill.billing.payment.plugin.api.PaymentTransactionInfoPlugin;
import org.killbill.billing.util.callcontext.CallContext;
import org.killbill.billing.util.callcontext.TenantContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.math.BigDecimal;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;


public class PaymentProcessor{

    private static final ImmutableList<PluginProperty> PLUGIN_PROPERTIES = ImmutableList.of();

    private final PaymentAutomatonRunner paymentAutomatonRunner;
    private ProcessorBase processorBase;
    private PaymentLocator paymentLocator;

    private static final Logger log = LoggerFactory.getLogger(PaymentProcessor.class);


    @Inject
    public PaymentProcessor(final PaymentAutomatonRunner paymentAutomatonRunner,
                            final ProcessorBase processorBase,
                            final PaymentLocator paymentLocator) {
        this.paymentAutomatonRunner = paymentAutomatonRunner;
        this.processorBase = processorBase;
        this.paymentLocator = paymentLocator;
    }

    public Payment createAuthorization(final boolean isApiPayment, @Nullable final UUID attemptId, final Account account, @Nullable final UUID paymentMethodId, @Nullable final UUID paymentId, final BigDecimal amount, final Currency currency,
                                       @Nullable final String paymentExternalKey, @Nullable final String paymentTransactionExternalKey, @Nullable final UUID paymentIdForNewPayment, @Nullable final UUID paymentTransactionIdForNewPaymentTransaction, final boolean shouldLockAccountAndDispatch,
                                       final Iterable<PluginProperty> properties, final CallContext callContext, final InternalCallContext internalCallContext) throws PaymentApiException {
        return performOperation(isApiPayment, attemptId, TransactionType.AUTHORIZE, account, paymentMethodId, paymentId, null, amount, currency, paymentExternalKey, paymentTransactionExternalKey, paymentIdForNewPayment, paymentTransactionIdForNewPaymentTransaction, shouldLockAccountAndDispatch, null, properties, callContext, internalCallContext);
    }

    public Payment createCapture(final boolean isApiPayment, @Nullable final UUID attemptId, final Account account, final UUID paymentId, final BigDecimal amount, final Currency currency,
                                 @Nullable final String paymentTransactionExternalKey, @Nullable final UUID paymentTransactionIdForNewPaymentTransaction, final boolean shouldLockAccountAndDispatch,
                                 final Iterable<PluginProperty> properties, final CallContext callContext, final InternalCallContext internalCallContext) throws PaymentApiException {
        return performOperation(isApiPayment, attemptId, TransactionType.CAPTURE, account, null, paymentId, null, amount, currency, null, paymentTransactionExternalKey, null, paymentTransactionIdForNewPaymentTransaction, shouldLockAccountAndDispatch, null, properties, callContext, internalCallContext);
    }

    public Payment createPurchase(final boolean isApiPayment, @Nullable final UUID attemptId, final Account account, @Nullable final UUID paymentMethodId, @Nullable final UUID paymentId, final BigDecimal amount, final Currency currency,
                                  @Nullable final String paymentExternalKey, @Nullable final String paymentTransactionExternalKey, @Nullable final UUID paymentIdForNewPayment, @Nullable final UUID paymentTransactionIdForNewPaymentTransaction, final boolean shouldLockAccountAndDispatch,
                                  final Iterable<PluginProperty> properties, final CallContext callContext, final InternalCallContext internalCallContext) throws PaymentApiException {
        return performOperation(isApiPayment, attemptId, TransactionType.PURCHASE, account, paymentMethodId, paymentId, null, amount, currency, paymentExternalKey, paymentTransactionExternalKey, paymentIdForNewPayment, paymentTransactionIdForNewPaymentTransaction, shouldLockAccountAndDispatch, null, properties, callContext, internalCallContext);
    }

    public Payment createVoid(final boolean isApiPayment, @Nullable final UUID attemptId, final Account account, final UUID paymentId, @Nullable final String paymentTransactionExternalKey, @Nullable final UUID paymentTransactionIdForNewPaymentTransaction, final boolean shouldLockAccountAndDispatch,
                              final Iterable<PluginProperty> properties, final CallContext callContext, final InternalCallContext internalCallContext) throws PaymentApiException {
        return performOperation(isApiPayment, attemptId, TransactionType.VOID, account, null, paymentId, null, null, null, null, paymentTransactionExternalKey, null, paymentTransactionIdForNewPaymentTransaction, shouldLockAccountAndDispatch, null, properties, callContext, internalCallContext);
    }

    public Payment createRefund(final boolean isApiPayment, @Nullable final UUID attemptId, final Account account, final UUID paymentId, final BigDecimal amount, final Currency currency,
                                final String paymentTransactionExternalKey, @Nullable final UUID paymentTransactionIdForNewPaymentTransaction, final boolean shouldLockAccountAndDispatch,
                                final Iterable<PluginProperty> properties, final CallContext callContext, final InternalCallContext internalCallContext) throws PaymentApiException {
        return performOperation(isApiPayment, attemptId, TransactionType.REFUND, account, null, paymentId, null, amount, currency, null, paymentTransactionExternalKey, null, paymentTransactionIdForNewPaymentTransaction, shouldLockAccountAndDispatch, null, properties, callContext, internalCallContext);
    }

    public Payment createCredit(final boolean isApiPayment, @Nullable final UUID attemptId, final Account account, @Nullable final UUID paymentMethodId, @Nullable final UUID paymentId, final BigDecimal amount, final Currency currency,
                                @Nullable final String paymentExternalKey, @Nullable final String paymentTransactionExternalKey, @Nullable final UUID paymentIdForNewPayment, @Nullable final UUID paymentTransactionIdForNewPaymentTransaction, final boolean shouldLockAccountAndDispatch,
                                final Iterable<PluginProperty> properties, final CallContext callContext, final InternalCallContext internalCallContext) throws PaymentApiException {
        return performOperation(isApiPayment, attemptId, TransactionType.CREDIT, account, paymentMethodId, paymentId, null, amount, currency, paymentExternalKey, paymentTransactionExternalKey, paymentIdForNewPayment, paymentTransactionIdForNewPaymentTransaction, shouldLockAccountAndDispatch, null, properties, callContext, internalCallContext);
    }

    public Payment createChargeback(final boolean isApiPayment, @Nullable final UUID attemptId, final Account account, final UUID paymentId, @Nullable final String paymentTransactionExternalKey, final BigDecimal amount, final Currency currency, @Nullable final UUID paymentTransactionIdForNewPaymentTransaction, final boolean shouldLockAccountAndDispatch,
                                    final CallContext callContext, final InternalCallContext internalCallContext) throws PaymentApiException {
        return performOperation(isApiPayment, attemptId, TransactionType.CHARGEBACK, account, null, paymentId, null, amount, currency, null, paymentTransactionExternalKey, null, paymentTransactionIdForNewPaymentTransaction, shouldLockAccountAndDispatch, null, PLUGIN_PROPERTIES, callContext, internalCallContext);
    }

    public Payment createChargebackReversal(final boolean isApiPayment, @Nullable final UUID attemptId, final Account account, final UUID paymentId, @Nullable final String paymentTransactionExternalKey, final BigDecimal amount, final Currency currency, @Nullable final UUID paymentTransactionIdForNewPaymentTransaction, final boolean shouldLockAccountAndDispatch,
                                            final CallContext callContext, final InternalCallContext internalCallContext) throws PaymentApiException {
        return performOperation(isApiPayment, attemptId, TransactionType.CHARGEBACK, account, null, paymentId, null, amount, currency, null, paymentTransactionExternalKey, null, paymentTransactionIdForNewPaymentTransaction, shouldLockAccountAndDispatch, OperationResult.FAILURE, PLUGIN_PROPERTIES, callContext, internalCallContext);
    }

    public Payment notifyPendingPaymentOfStateChanged(final Account account, final UUID transactionId, final boolean isSuccess, final CallContext callContext, final InternalCallContext internalCallContext) throws PaymentApiException {
        final PaymentTransactionModelDao transactionModelDao = processorBase.getPaymentDao().getPaymentTransaction(transactionId, internalCallContext);
        if (transactionModelDao.getTransactionStatus() != TransactionStatus.PENDING) {
            throw new PaymentApiException(ErrorCode.PAYMENT_NO_SUCH_SUCCESS_PAYMENT, transactionModelDao.getPaymentId());
        }

        final OperationResult overridePluginResult = isSuccess ? OperationResult.SUCCESS : OperationResult.FAILURE;

        return performOperation(true, false, null, transactionModelDao.getTransactionType(), account, null, transactionModelDao.getPaymentId(),
                                transactionModelDao.getId(), transactionModelDao.getAmount(), transactionModelDao.getCurrency(), null, transactionModelDao.getTransactionExternalKey(), null, null, true,
                                overridePluginResult, PLUGIN_PROPERTIES, callContext, internalCallContext);
    }

    private Payment performOperation(final boolean isApiPayment,
                                     @Nullable final UUID attemptId,
                                     final TransactionType transactionType,
                                     final Account account,
                                     @Nullable final UUID paymentMethodId,
                                     @Nullable final UUID paymentId,
                                     @Nullable final UUID transactionId,
                                     @Nullable final BigDecimal amount,
                                     @Nullable final Currency currency,
                                     @Nullable final String paymentExternalKey,
                                     @Nullable final String paymentTransactionExternalKey,
                                     @Nullable final UUID paymentIdForNewPayment,
                                     @Nullable final UUID paymentTransactionIdForNewPaymentTransaction,
                                     final boolean shouldLockAccountAndDispatch,
                                     @Nullable final OperationResult overridePluginOperationResult,
                                     final Iterable<PluginProperty> properties,
                                     final CallContext callContext,
                                     final InternalCallContext internalCallContext) throws PaymentApiException {
        return performOperation(isApiPayment, true, attemptId, transactionType, account, paymentMethodId, paymentId,
                                transactionId, amount, currency, paymentExternalKey, paymentTransactionExternalKey,
                                paymentIdForNewPayment, paymentTransactionIdForNewPaymentTransaction, shouldLockAccountAndDispatch, overridePluginOperationResult, properties, callContext, internalCallContext);
    }

    private Payment performOperation(final boolean isApiPayment,
                                     final boolean runJanitor,
                                     @Nullable final UUID attemptId,
                                     final TransactionType transactionType,
                                     final Account account,
                                     @Nullable final UUID paymentMethodId,
                                     @Nullable final UUID paymentId,
                                     @Nullable final UUID transactionId,
                                     @Nullable final BigDecimal amount,
                                     @Nullable final Currency currency,
                                     @Nullable final String paymentExternalKey,
                                     @Nullable final String paymentTransactionExternalKey,
                                     @Nullable final UUID paymentIdForNewPayment,
                                     @Nullable final UUID paymentTransactionIdForNewPaymentTransaction,
                                     final boolean shouldLockAccountAndDispatch,
                                     @Nullable final OperationResult overridePluginOperationResult,
                                     final Iterable<PluginProperty> properties,
                                     final CallContext callContext,
                                     final InternalCallContext internalCallContext) throws PaymentApiException {
        final PaymentStateContext paymentStateContext = paymentAutomatonRunner.buildPaymentStateContext(isApiPayment,
                                                                                                        transactionType,
                                                                                                        account,
                                                                                                        attemptId,
                                                                                                        paymentMethodId != null ? paymentMethodId : account.getPaymentMethodId(),
                                                                                                        paymentId,
                                                                                                        transactionId,
                                                                                                        paymentExternalKey,
                                                                                                        paymentTransactionExternalKey,
                                                                                                        amount,
                                                                                                        currency,
                                                                                                        paymentIdForNewPayment,
                                                                                                        paymentTransactionIdForNewPaymentTransaction,
                                                                                                        shouldLockAccountAndDispatch,
                                                                                                        overridePluginOperationResult,
                                                                                                        properties,
                                                                                                        callContext,
                                                                                                        internalCallContext);
        final PaymentAutomatonDAOHelper daoHelper = paymentAutomatonRunner.buildDaoHelper(paymentStateContext, internalCallContext);

        String currentStateName = null;
        if (paymentStateContext.getPaymentId() != null) {
            PaymentModelDao paymentModelDao = daoHelper.getPayment();

            // Sanity: verify the payment belongs to the right account (in case it was looked-up by payment or transaction external key)
            if (!paymentModelDao.getAccountRecordId().equals(internalCallContext.getAccountRecordId())) {
                throw new PaymentApiException(ErrorCode.PAYMENT_DIFFERENT_ACCOUNT_ID, paymentStateContext.getPaymentId());
            }

            // Note: the list needs to be modifiable for invokeJanitor
            final Collection<PaymentTransactionModelDao> paymentTransactionsForCurrentPayment = new LinkedList<PaymentTransactionModelDao>(daoHelper.getPaymentDao().getTransactionsForPayment(paymentStateContext.getPaymentId(), paymentStateContext.getInternalCallContext()));
            // Always invoke the Janitor first to get the latest state. The state machine will then
            // prevent disallowed transitions in case the state couldn't be fixed (or if it's already in a final state).
            if (runJanitor) {
                final PaymentPluginApi plugin = processorBase.getPaymentProviderPlugin(paymentModelDao.getPaymentMethodId(), true, internalCallContext);
                final List<PaymentTransactionInfoPlugin> pluginTransactions = getPaymentTransactionInfoPlugins(plugin, paymentModelDao, properties, callContext);
                paymentModelDao = paymentLocator.invokeJanitor(paymentModelDao, paymentTransactionsForCurrentPayment, pluginTransactions, internalCallContext);
            }

            if (paymentStateContext.getPaymentTransactionExternalKey() != null) {
                final List<PaymentTransactionModelDao> allPaymentTransactionsForKey = daoHelper.getPaymentDao().getPaymentTransactionsByExternalKey(paymentStateContext.getPaymentTransactionExternalKey(), internalCallContext);
                runSanityOnTransactionExternalKey(allPaymentTransactionsForKey, paymentStateContext, internalCallContext);
            }

            if (paymentStateContext.getTransactionId() != null || paymentStateContext.getPaymentTransactionExternalKey() != null) {
                // If a transaction id or key is passed, we are maybe completing an existing transaction (unless a new key was provided)
                PaymentTransactionModelDao transactionToComplete = findTransactionToCompleteAndRunSanityChecks(paymentModelDao, paymentTransactionsForCurrentPayment, paymentStateContext);

                if (transactionToComplete != null) {
                    final UUID transactionToCompleteId = transactionToComplete.getId();
                    transactionToComplete = Iterables.find(paymentTransactionsForCurrentPayment,
                                                                                       new Predicate<PaymentTransactionModelDao>() {
                                                                                           @Override
                                                                                           public boolean apply(final PaymentTransactionModelDao input) {
                                                                                               return transactionToCompleteId.equals(input.getId());
                                                                                           }
                                                                                       });

                    // We can't tell where we should be in the state machine - bail (cannot be enforced by the state machine unfortunately because UNKNOWN and PLUGIN_FAILURE are both treated as EXCEPTION)
                    if (transactionToComplete.getTransactionStatus() == TransactionStatus.UNKNOWN) {
                        throw new PaymentApiException(ErrorCode.PAYMENT_INVALID_OPERATION, paymentStateContext.getTransactionType(), transactionToComplete.getTransactionStatus());
                    }

                    paymentStateContext.setPaymentTransactionModelDao(transactionToComplete);
                }
            }

            // Use the original payment method id of the payment being completed
            paymentStateContext.setPaymentMethodId(paymentModelDao.getPaymentMethodId());
            // We always take the last successful state name to permit retries on failures
            currentStateName = paymentModelDao.getLastSuccessStateName();
        }

        paymentAutomatonRunner.run(paymentStateContext, daoHelper, currentStateName, transactionType);

        return paymentLocator.getPayment(paymentStateContext.getPaymentModelDao(), true, false, properties, callContext, internalCallContext);
    }

    private void runSanityOnTransactionExternalKey(final Iterable<PaymentTransactionModelDao> allPaymentTransactionsForKey,
                                                   final PaymentStateContext paymentStateContext,
                                                   final InternalCallContext internalCallContext) throws PaymentApiException {
        for (final PaymentTransactionModelDao paymentTransactionModelDao : allPaymentTransactionsForKey) {
            // Sanity: verify we don't already have a successful transaction for that key (chargeback reversals are a bit special, it's the only transaction type we can revert)
            if (paymentTransactionModelDao.getTransactionExternalKey().equals(paymentStateContext.getPaymentTransactionExternalKey()) &&
                paymentTransactionModelDao.getTransactionStatus() == TransactionStatus.SUCCESS &&
                paymentTransactionModelDao.getTransactionType() != TransactionType.CHARGEBACK) {
                throw new PaymentApiException(ErrorCode.PAYMENT_ACTIVE_TRANSACTION_KEY_EXISTS, paymentStateContext.getPaymentTransactionExternalKey());
            }

            // Sanity: don't share keys across accounts
            if (!paymentTransactionModelDao.getAccountRecordId().equals(internalCallContext.getAccountRecordId())) {
                UUID accountId;
                try {
                    accountId = processorBase.getAccountInternalApi().getAccountByRecordId(paymentTransactionModelDao.getAccountRecordId(), internalCallContext).getId();
                } catch (final AccountApiException e) {
                    log.warn("Unable to retrieve account", e);
                    accountId = null;
                }
                throw new PaymentApiException(ErrorCode.PAYMENT_TRANSACTION_DIFFERENT_ACCOUNT_ID, accountId);
            }
        }
    }

    private PaymentTransactionModelDao findTransactionToCompleteAndRunSanityChecks(final PaymentModelDao paymentModelDao,
                                                                                   final Iterable<PaymentTransactionModelDao> paymentTransactionsForCurrentPayment,
                                                                                   final PaymentStateContext paymentStateContext) throws PaymentApiException {
        final Collection<PaymentTransactionModelDao> completionCandidates = new LinkedList<PaymentTransactionModelDao>();
        for (final PaymentTransactionModelDao paymentTransactionModelDao : paymentTransactionsForCurrentPayment) {
            // Check if we already have a transaction for that id or key
            if (!(paymentStateContext.getTransactionId() != null && paymentTransactionModelDao.getId().equals(paymentStateContext.getTransactionId())) &&
                !(paymentStateContext.getPaymentTransactionExternalKey() != null && paymentTransactionModelDao.getTransactionExternalKey().equals(paymentStateContext.getPaymentTransactionExternalKey()))) {
                // Sanity: if not, prevent multiple PENDING transactions for initial calls (cannot be enforced by the state machine unfortunately)
                if ((paymentTransactionModelDao.getTransactionType() == TransactionType.AUTHORIZE ||
                     paymentTransactionModelDao.getTransactionType() == TransactionType.PURCHASE ||
                     paymentTransactionModelDao.getTransactionType() == TransactionType.CREDIT) &&
                    paymentTransactionModelDao.getTransactionStatus() == TransactionStatus.PENDING) {
                    throw new PaymentApiException(ErrorCode.PAYMENT_INVALID_OPERATION, paymentTransactionModelDao.getTransactionType(), paymentModelDao.getStateName());
                } else {
                    continue;
                }
            }

            // Sanity: if we already have a transaction for that id or key, the transaction type must match
            if (paymentTransactionModelDao.getTransactionType() != paymentStateContext.getTransactionType()) {
                throw new PaymentApiException(ErrorCode.PAYMENT_INVALID_PARAMETER, "transactionType", String.format("%s doesn't match existing transaction type %s", paymentStateContext.getTransactionType(), paymentTransactionModelDao.getTransactionType()));
            }

            // UNKNOWN transactions are potential candidates, we'll invoke the Janitor first though
            if (paymentTransactionModelDao.getTransactionStatus() == TransactionStatus.PENDING || paymentTransactionModelDao.getTransactionStatus() == TransactionStatus.UNKNOWN) {
                completionCandidates.add(paymentTransactionModelDao);
            }
        }

        Preconditions.checkState(Iterables.<PaymentTransactionModelDao>size(completionCandidates) <= 1, "There should be at most one completion candidate");
        return Iterables.getLast(completionCandidates, null);
    }

    private List<PaymentTransactionInfoPlugin> getPaymentTransactionInfoPlugins(final PaymentPluginApi plugin, final PaymentModelDao paymentModelDao, final Iterable<PluginProperty> properties, final TenantContext context) throws PaymentApiException {
        try {
            return plugin.getPaymentInfo(paymentModelDao.getAccountId(), paymentModelDao.getId(), properties, context);
        } catch (final PaymentPluginApiException e) {
            throw new PaymentApiException(e, ErrorCode.PAYMENT_PLUGIN_GET_PAYMENT_INFO, paymentModelDao.getId(), e.toString());
        }
    }
}

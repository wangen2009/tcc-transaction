package org.mengyun.tcctransaction.recover;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.Logger;
import org.mengyun.tcctransaction.OptimisticLockException;
import org.mengyun.tcctransaction.Transaction;
import org.mengyun.tcctransaction.TransactionRepository;
import org.mengyun.tcctransaction.api.TransactionStatus;
import org.mengyun.tcctransaction.common.TransactionType;
import org.mengyun.tcctransaction.support.TransactionConfigurator;

import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * 异常事务恢复
 *
 * Created by changmingxie on 11/10/15.
 */
public class TransactionRecovery {

    static final Logger logger = Logger.getLogger(TransactionRecovery.class.getSimpleName());

    /**
     * TCC事务配置器
     */
    private TransactionConfigurator transactionConfigurator;

    /**
     * 启动事务恢复操作(被RecoverScheduledJob定时任务调用).
     */
    public void startRecover() {
        // 找出所有执行错误的事务信息
        List<Transaction> transactions = loadErrorTransactions();
        // 恢复异常事务集合
        recoverErrorTransactions(transactions);
    }

    /**
     * 加载异常事务集合
     * 异常的定义：超过事务恢复间隔时间未完成的事务。
     *
     * @return 异常事务集合
     */
    private List<Transaction> loadErrorTransactions() {
        TransactionRepository transactionRepository = transactionConfigurator.getTransactionRepository();
        long currentTimeInMillis = Calendar.getInstance().getTimeInMillis();
        RecoverConfig recoverConfig = transactionConfigurator.getRecoverConfig();
        return transactionRepository.findAllUnmodifiedSince(new Date(currentTimeInMillis - recoverConfig.getRecoverDuration() * 1000));
    }

    /**
     * 恢复异常事务集合
     *
     * @param transactions 异常事务结合
     */
    private void recoverErrorTransactions(List<Transaction> transactions) {
        for (Transaction transaction : transactions) {
            // 超过最大重试次数
            if (transaction.getRetriedCount() > transactionConfigurator.getRecoverConfig().getMaxRetryCount()) {
                logger.error(String.format("recover failed with max retry count,will not try again. txid:%s, status:%s,retried count:%d,transaction content:%s", transaction.getXid(), transaction.getStatus().getId(), transaction.getRetriedCount(), JSON.toJSONString(transaction)));
                continue;
            }
            // 分支事务超过最大可重试时间
            if (transaction.getTransactionType().equals(TransactionType.BRANCH)
                    && (transaction.getCreateTime().getTime() +
                    transactionConfigurator.getRecoverConfig().getMaxRetryCount() *
                            transactionConfigurator.getRecoverConfig().getRecoverDuration() * 1000
                    > System.currentTimeMillis())) {
                continue;
            }
            // Confirm / Cancel
            try {
                // 增加重试次数
                transaction.addRetriedCount();
                // Confirm
                if (transaction.getStatus().equals(TransactionStatus.CONFIRMING)) {
                    // 如果是CONFIRMING(2)状态，则将事务往前执行
                    transaction.changeStatus(TransactionStatus.CONFIRMING);
                    transactionConfigurator.getTransactionRepository().update(transaction);
                    transaction.commit();
                    // 其他情况下，超时没处理的事务日志直接删除
                    transactionConfigurator.getTransactionRepository().delete(transaction);
                // Cancel
                } else if (transaction.getStatus().equals(TransactionStatus.CANCELLING)
                        || transaction.getTransactionType().equals(TransactionType.ROOT)) { // 处理延迟取消的情况
                    // 其他情况，把事务状态改为CANCELLING(3)，然后执行回滚
                    transaction.changeStatus(TransactionStatus.CANCELLING);
                    transactionConfigurator.getTransactionRepository().update(transaction);
                    transaction.rollback();
                    // 其他情况下，超时没处理的事务日志直接删除
                    transactionConfigurator.getTransactionRepository().delete(transaction);
                }
            } catch (Throwable throwable) {
                if (throwable instanceof OptimisticLockException
                        || ExceptionUtils.getRootCause(throwable) instanceof OptimisticLockException) {
                    logger.warn(String.format("optimisticLockException happened while recover. txid:%s, status:%s,retried count:%d,transaction content:%s", transaction.getXid(), transaction.getStatus().getId(), transaction.getRetriedCount(), JSON.toJSONString(transaction)), throwable);
                } else {
                    logger.error(String.format("recover failed, txid:%s, status:%s,retried count:%d,transaction content:%s", transaction.getXid(), transaction.getStatus().getId(), transaction.getRetriedCount(), JSON.toJSONString(transaction)), throwable);
                }
            }
        }
    }

    /**
     * 设置事务配置器.
     * @param transactionConfigurator
     */
    public void setTransactionConfigurator(TransactionConfigurator transactionConfigurator) {
        this.transactionConfigurator = transactionConfigurator;
    }

}

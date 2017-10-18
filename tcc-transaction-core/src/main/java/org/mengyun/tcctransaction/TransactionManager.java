package org.mengyun.tcctransaction;

import org.apache.log4j.Logger;
import org.mengyun.tcctransaction.api.TransactionContext;
import org.mengyun.tcctransaction.api.TransactionStatus;
import org.mengyun.tcctransaction.common.TransactionType;

import java.util.Deque;
import java.util.LinkedList;

/**
 * 事务管理器
 *
 * Created by changmingxie on 10/26/15.
 */
public class TransactionManager {

    static final Logger logger = Logger.getLogger(TransactionManager.class.getSimpleName());

    /**
     * 事务存储器
     */
    private TransactionRepository transactionRepository;
    /**
     * 当前线程事务队列
     */
    private static final ThreadLocal<Deque<Transaction>> CURRENT = new ThreadLocal<Deque<Transaction>>();

    public void setTransactionRepository(TransactionRepository transactionRepository) {
        this.transactionRepository = transactionRepository;
    }

    /**
     * 事务开始（创建事务日志记录，并将该事务日志记录存入当前线程的事务局部变量中）
     *
     * @return 事务
     */
    public Transaction begin() {
        // 创建 根事务
        Transaction transaction = new Transaction(TransactionType.ROOT);
        // 存储 事务
        transactionRepository.create(transaction);// 创建事务记录,写入事务日志库
        // 注册 事务
        registerTransaction(transaction);
        return transaction;
    }

    /**
     * 基于全局事务ID扩展创建新的分支事务，并存于当前线程的事务局部变量中.
     *
     * @param transactionContext 事务上下文
     * @return 分支事务
     */
    public Transaction propagationNewBegin(TransactionContext transactionContext) {
        // 创建 分支事务
        Transaction transaction = new Transaction(transactionContext);
        // 存储 事务
        transactionRepository.create(transaction);
        // 注册 事务
        registerTransaction(transaction);
        return transaction;
    }

    /**
     * 找出存在的事务并处理
     *
     * @param transactionContext 事务上下文
     * @return 分支事务
     * @throws NoExistedTransactionException 当事务不存在时
     */
    public Transaction propagationExistBegin(TransactionContext transactionContext) throws NoExistedTransactionException {
        // 查询 事务
        Transaction transaction = transactionRepository.findByXid(transactionContext.getXid());
        if (transaction != null) {
            // 设置 事务 状态
            transaction.changeStatus(TransactionStatus.valueOf(transactionContext.getStatus()));
            // 注册 事务
            registerTransaction(transaction);
            return transaction;
        } else {
            throw new NoExistedTransactionException();
        }
    }

    /**
     * 提交事务
     */
    public void commit() {
        // 获取 事务
        Transaction transaction = getCurrentTransaction();
        // 设置 事务状态 为 CONFIRMING
        transaction.changeStatus(TransactionStatus.CONFIRMING);
        // 更新 事务
        transactionRepository.update(transaction);
        try {
            // 提交 事务
            transaction.commit();
            // 删除 事务
            transactionRepository.delete(transaction);
        } catch (Throwable commitException) {
            logger.error("compensable transaction confirm failed.", commitException);
            throw new ConfirmingException(commitException);
        }
    }

    /**
     * 回滚事务
     */
    public void rollback() {
        // 获取 事务
        Transaction transaction = getCurrentTransaction();
        // 设置 事务状态 为 CANCELLING
        transaction.changeStatus(TransactionStatus.CANCELLING);
        // 更新 事务
        transactionRepository.update(transaction);
        try {
            // 提交 事务
            transaction.rollback();
            // 删除 事务
            transactionRepository.delete(transaction);
        } catch (Throwable rollbackException) {
            logger.error("compensable transaction rollback failed.", rollbackException);
            throw new CancellingException(rollbackException);
        }
    }

    /**
     * 注册事务到当前线程事务队列
     *
     * @param transaction 事务
     */
    private void registerTransaction(Transaction transaction) {
        if (CURRENT.get() == null) {
            CURRENT.set(new LinkedList<Transaction>());
        }
        CURRENT.get().push(transaction); // 添加到头部
    }

    /**
     * 获取当前线程事务第一个(头部)元素
     *
     * @return 事务
     */
    public Transaction getCurrentTransaction() {
        if (isTransactionActive()) {
            return CURRENT.get().peek(); // 获得头部元素
        }
        return null;
    }

    /**
     * 当前线程是否在事务中
     *
     * @return 是否在事务中
     */
    public boolean isTransactionActive() {
        Deque<Transaction> transactions = CURRENT.get();
        return transactions != null && !transactions.isEmpty();
    }

    /**
     * 将事务从当前线程事务队列移除
     *
     * @param transaction 事务
     */
    public void cleanAfterCompletion(Transaction transaction) {
        if (isTransactionActive() && transaction != null) {
            Transaction currentTransaction = getCurrentTransaction();
            if (currentTransaction == transaction) {
                CURRENT.get().pop();
            } else {
                throw new SystemException("Illegal transaction when clean after completion");
            }
        }
    }

    /**
     * 添加参与者到事务
     *
     * @param participant 参与者
     */
    public void enlistParticipant(Participant participant) {
        // 获取 事务
        Transaction transaction = this.getCurrentTransaction();
        // 添加参与者
        transaction.enlistParticipant(participant);
        // 更新 事务
        transactionRepository.update(transaction);
    }
}

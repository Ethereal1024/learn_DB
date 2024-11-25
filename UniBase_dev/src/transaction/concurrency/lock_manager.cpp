#include "lock_manager.h"

/**
 * @description: 申请行级共享锁
 * @return {bool} 加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {Rid&} rid 加锁的目标记录ID 记录所在的表的fd
 * @param {int} tab_fd
 */
bool LockManager::lock_shared_on_record(Transaction* txn, const Rid& rid, int tab_fd) {
    std::unique_lock<std::mutex> lock(latch_);
    // 读未提交的级别不支持加锁
    if (txn->get_isolation_level() == IsolationLevel::READ_UNCOMMITTED) {
        txn->set_state(TransactionState::ABORTED);
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
    }

    auto lockDataId = LockDataId(tab_fd, rid, LockDataType::RECORD);
    txn->get_lock_set()->insert(lockDataId);
    txn->set_state(TransactionState::GROWING);

    LockRequest* request = new LockRequest(txn->get_transaction_id(), LockMode::SHARED);
    lock_table_[lockDataId].request_queue_.push_back(*request);

    while (
        lock_table_[lockDataId].group_lock_mode_ != GroupLockMode::NON_LOCK &&
        lock_table_[lockDataId].group_lock_mode_ != GroupLockMode::S) {
        lock_table_[lockDataId].cv_.wait(lock);  // S锁只与S锁相容
    }

    request->granted_ = true;
    lock_table_[lockDataId].group_lock_mode_ = GroupLockMode::S;
    lock_table_[lockDataId].cv_.notify_all();
    return true;
}

/**
 * @description: 申请行级排他锁
 * @return {bool} 加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {Rid&} rid 加锁的目标记录ID
 * @param {int} tab_fd 记录所在的表的fd
 */
bool LockManager::lock_exclusive_on_record(Transaction* txn, const Rid& rid, int tab_fd) {
    std::unique_lock<std::mutex> lock(latch_);
    // 读未提交的级别不支持加锁
    if (txn->get_isolation_level() == IsolationLevel::READ_UNCOMMITTED) {
        txn->set_state(TransactionState::ABORTED);
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
    }

    auto lockDataId = LockDataId(tab_fd, rid, LockDataType::RECORD);
    txn->get_lock_set()->insert(lockDataId);
    txn->set_state(TransactionState::GROWING);

    LockRequest* request = new LockRequest(txn->get_transaction_id(), LockMode::EXLUCSIVE);
    lock_table_[lockDataId].request_queue_.push_back(*request);

    while (lock_table_[lockDataId].group_lock_mode_ != GroupLockMode::NON_LOCK) {
        lock_table_[lockDataId].cv_.wait(lock);  // X锁不与其他任何锁相容
    }

    request->granted_ = true;
    lock_table_[lockDataId].group_lock_mode_ = GroupLockMode::X;
    lock_table_[lockDataId].cv_.notify_all();
    return true;
}

/**
 * @description: 申请表级读锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_shared_on_table(Transaction* txn, int tab_fd) {
    std::unique_lock<std::mutex> lock(latch_);
    // 读未提交的级别不支持加锁
    if (txn->get_isolation_level() == IsolationLevel::READ_UNCOMMITTED) {
        txn->set_state(TransactionState::ABORTED);
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
    }

    auto lockDataId = LockDataId(tab_fd, LockDataType::TABLE);
    txn->get_lock_set()->insert(lockDataId);
    txn->set_state(TransactionState::GROWING);

    LockRequest* request = new LockRequest(txn->get_transaction_id(), LockMode::SHARED);
    lock_table_[lockDataId].request_queue_.push_back(*request);

    while (
        lock_table_[lockDataId].group_lock_mode_ != GroupLockMode::NON_LOCK &&
        lock_table_[lockDataId].group_lock_mode_ != GroupLockMode::S &&
        lock_table_[lockDataId].group_lock_mode_ != GroupLockMode::IS) {
        lock_table_[lockDataId].cv_.wait(lock);  // S锁只和IS锁及S锁相容
    }

    request->granted_ = true;

    if (lock_table_[lockDataId].group_lock_mode_ == GroupLockMode::NON_LOCK ||
        lock_table_[lockDataId].group_lock_mode_ == GroupLockMode::IS) {
        lock_table_[lockDataId].group_lock_mode_ = GroupLockMode::S;
    } else if (lock_table_[lockDataId].group_lock_mode_ == GroupLockMode::IX) {
        lock_table_[lockDataId].group_lock_mode_ = GroupLockMode::SIX;
    }

    lock_table_[lockDataId].cv_.notify_all();
    return true;
}

/**
 * @description: 申请表级写锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_exclusive_on_table(Transaction* txn, int tab_fd) {
    std::unique_lock<std::mutex> lock(latch_);
    // 读未提交的级别不支持加锁
    if (txn->get_isolation_level() == IsolationLevel::READ_UNCOMMITTED) {
        txn->set_state(TransactionState::ABORTED);
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
    }

    auto lockDataId = LockDataId(tab_fd, LockDataType::TABLE);
    txn->get_lock_set()->insert(lockDataId);
    txn->set_state(TransactionState::GROWING);

    LockRequest* request = new LockRequest(txn->get_transaction_id(), LockMode::EXLUCSIVE);
    lock_table_[lockDataId].request_queue_.push_back(*request);

    while (lock_table_[lockDataId].group_lock_mode_ != GroupLockMode::NON_LOCK) {
        lock_table_[lockDataId].cv_.wait(lock);  // X锁不与其他任何锁相容
    }

    request->granted_ = true;
    lock_table_[lockDataId].group_lock_mode_ = GroupLockMode::X;
    lock_table_[lockDataId].cv_.notify_all();
    return true;
}

/**
 * @description: 申请表级意向读锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_IS_on_table(Transaction* txn, int tab_fd) {
    std::unique_lock<std::mutex> lock(latch_);
    // 读未提交的级别不支持加锁
    if (txn->get_isolation_level() == IsolationLevel::READ_UNCOMMITTED) {
        txn->set_state(TransactionState::ABORTED);
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
    }

    auto lockDataId = LockDataId(tab_fd, LockDataType::TABLE);
    txn->get_lock_set()->insert(lockDataId);
    txn->set_state(TransactionState::GROWING);

    LockRequest* request = new LockRequest(txn->get_transaction_id(), LockMode::INTENTION_SHARED);
    lock_table_[lockDataId].request_queue_.push_back(*request);

    while (lock_table_[lockDataId].group_lock_mode_ == GroupLockMode::X) {
        lock_table_[lockDataId].cv_.wait(lock);  // IS锁与除X锁外的其他锁都相容
    }

    request->granted_ = true;
    if (lock_table_[lockDataId].group_lock_mode_ == GroupLockMode::NON_LOCK) {
        lock_table_[lockDataId].group_lock_mode_ = GroupLockMode::IS;
    }
    lock_table_[lockDataId].cv_.notify_all();
    return true;
}

/**
 * @description: 申请表级意向写锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_IX_on_table(Transaction* txn, int tab_fd) {
    std::unique_lock<std::mutex> lock(latch_);
    // 读未提交的级别不支持加锁
    if (txn->get_isolation_level() == IsolationLevel::READ_UNCOMMITTED) {
        txn->set_state(TransactionState::ABORTED);
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
    }

    auto lockDataId = LockDataId(tab_fd, LockDataType::TABLE);
    txn->get_lock_set()->insert(lockDataId);
    txn->set_state(TransactionState::GROWING);

    LockRequest* request = new LockRequest(txn->get_transaction_id(), LockMode::INTENTION_EXCLUSIVE);
    lock_table_[lockDataId].request_queue_.push_back(*request);

    while (lock_table_[lockDataId].group_lock_mode_ == GroupLockMode::S ||
           lock_table_[lockDataId].group_lock_mode_ == GroupLockMode::SIX ||
           lock_table_[lockDataId].group_lock_mode_ == GroupLockMode::X) {
        lock_table_[lockDataId].cv_.wait(lock);  // IX锁与S锁，SIX锁及X锁不相容
    }

    request->granted_ = true;
    if (lock_table_[lockDataId].group_lock_mode_ == GroupLockMode::NON_LOCK ||
        lock_table_[lockDataId].group_lock_mode_ == GroupLockMode::IS) {
        lock_table_[lockDataId].group_lock_mode_ = GroupLockMode::IX;
    } else if (lock_table_[lockDataId].group_lock_mode_ == GroupLockMode::S) {
        lock_table_[lockDataId].group_lock_mode_ = GroupLockMode::SIX;
    }

    lock_table_[lockDataId].cv_.notify_all();
    return true;
}

/**
 * @description: 释放锁
 * @return {bool} 返回解锁是否成功
 * @param {Transaction*} txn 要释放锁的事务对象指针
 * @param {LockDataId} lock_data_id 要释放的锁ID
 */
bool LockManager::unlock(Transaction* txn, LockDataId lock_data_id) {
    std::unique_lock<std::mutex> lock(latch_);

    txn->set_state(TransactionState::SHRINKING);
    if (txn->get_lock_set()->find(lock_data_id) == txn->get_lock_set()->end()) {
        return false;
    }

    // 在加锁请求队列中找到对应的请求删除
    auto* requestQueue = &lock_table_[lock_data_id].request_queue_;
    auto request = requestQueue->begin();
    while (request != requestQueue->end()) {
        if (request->txn_id_ == txn->get_transaction_id()) {
            request = requestQueue->erase(request);
        } else {
            request++;
        }
    }

    // 根据请求队列的情况重新为该组分配锁的等级
    GroupLockMode mode = GroupLockMode::NON_LOCK;
    for (auto request : *requestQueue) {
        if (request.granted_) {
            if (request.lock_mode_ == LockMode::EXLUCSIVE) {
                mode = GroupLockMode::X;
                break;
            } else if (request.lock_mode_ == LockMode::S_IX) {
                mode = GroupLockMode::SIX;
            } else if (request.lock_mode_ == LockMode::SHARED && mode != GroupLockMode::SIX) {
                if (mode == GroupLockMode::IX) {
                    mode = GroupLockMode::SIX;
                } else {
                    mode = GroupLockMode::S;
                }
            } else if (request.lock_mode_ == LockMode::INTENTION_EXCLUSIVE && mode != GroupLockMode::SIX) {
                if (mode == GroupLockMode::S) {
                    mode = GroupLockMode::SIX;
                } else {
                    mode = GroupLockMode::IX;
                }
            } else if (request.lock_mode_ == LockMode::INTENTION_SHARED &&
                       (mode == GroupLockMode::NON_LOCK || mode == GroupLockMode::IS)) {
                mode = GroupLockMode::IS;
            }
        }
    }

    lock_table_[lock_data_id].group_lock_mode_ = mode;
    lock_table_[lock_data_id].cv_.notify_all();
    return true;
}
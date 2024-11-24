#include "transaction_manager.h"

#include "record/rm_file_handle.h"
#include "system/sm_manager.h"

std::unordered_map<txn_id_t, Transaction*> TransactionManager::txn_map = {};

/**
 * @description: 事务的开始方法
 * @return {Transaction*} 开始事务的指针
 * @param {Transaction*} txn 事务指针，空指针代表需要创建新事务，否则开始已有事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
Transaction* TransactionManager::begin(Transaction* txn, LogManager* log_manager) {
    // Todo:
    // 1. 判断传入事务参数是否为空指针
    // 2. 如果为空指针，创建新事务
    // 3. 把开始事务加入到全局事务表中
    // 4. 返回当前事务指针
    if (txn == nullptr) {
        txn = new Transaction(next_txn_id_++);
    }
    txn_map[txn->get_transaction_id()] = txn;

    if (log_manager != nullptr) {
        BeginLogRecord beginLog(txn->get_transaction_id());
        log_manager->add_log_to_buffer(&beginLog);
    }
    return txn;
}

/**
 * @description: 事务的提交方法
 * @param {Transaction*} txn 需要提交的事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
void TransactionManager::commit(Transaction* txn, LogManager* log_manager) {
    // Todo:
    // 1. 如果存在未提交的写操作，提交所有的写操作
    // 2. 释放所有锁
    // 3. 释放事务相关资源，eg.锁集
    // 4. 把事务日志刷入磁盘中
    // 5. 更新事务状态
    if (log_manager != nullptr) {
        CommitLogRecord commitLog(txn->get_transaction_id());
        log_manager->add_log_to_buffer(&commitLog);
    }

    auto writeSet = txn->get_write_set();
    if (writeSet->empty()) {  // 先置空，之后搞清楚了再写
    }
    writeSet->clear();

    auto lockSet = txn->get_lock_set();
    for (auto lockDataId : *lockSet) {
        lock_manager_->unlock(txn, lockDataId);
    }
    lockSet->clear();

    txn->set_state(TransactionState::COMMITTED);
    txn_map.erase(txn->get_transaction_id());
    delete txn;
}

/**
 * @description: 事务的终止（回滚）方法
 * @param {Transaction *} txn 需要回滚的事务
 * @param {LogManager} *log_manager 日志管理器指针
 */
void TransactionManager::abort(Transaction* txn, LogManager* log_manager) {
    // Todo:
    // 1. 回滚所有写操作
    // 2. 释放所有锁
    // 3. 清空事务相关资源，eg.锁集
    // 4. 把事务日志刷入磁盘中
    // 5. 更新事务状态
    if (txn == nullptr)
        return;
    auto writeSet = txn->get_write_set();
    while (!writeSet->empty()) {
        auto& item = writeSet->back();
        auto context = new Context(lock_manager_, log_manager, txn);
        switch (item->GetWriteType()) {
        case WType::INSERT_TUPLE:
            sm_manager_->rollback_insert(item->GetTableName(), item->GetRid(), context);
            break;
        case WType::UPDATE_TUPLE:
            sm_manager_->rollback_update(item->GetTableName(), item->GetRid(), item->GetRecord(), context);
            break;
        case WType::DELETE_TUPLE:
            sm_manager_->rollback_delete(item->GetTableName(), item->GetRecord(), context);
            break;
        default:
            break;
        }
        writeSet->pop_back();
    }
    writeSet->clear();

    auto lockSet = txn->get_lock_set();
    for(auto it : *lockSet){
        lock_manager_->unlock(txn, it);
    }
    lockSet->clear();
    txn->set_state(TransactionState::ABORTED);
}
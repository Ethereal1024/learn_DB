#include "rm_file_handle.h"

/**
 * @description: 获取当前表中记录号为rid的记录
 * @param {Rid&} rid 记录号，指定记录的位置
 * @param {Context*} context
 * @return {unique_ptr<RmRecord>} rid对应的记录对象指针
 */
std::unique_ptr<RmRecord> RmFileHandle::get_record(const Rid& rid, Context* context) const {
    // 上S锁
    context->lock_mgr_->lock_shared_on_record(context->txn_, rid, fd_);
    auto lockDataId = LockDataId(fd_, rid, LockDataType::RECORD);

    auto pageHandle = fetch_page_handle(rid.page_no);
    auto record = std::make_unique<RmRecord>(file_hdr_.record_size);
    if (!Bitmap::is_set(pageHandle.bitmap, rid.slot_no)) {
        throw RecordNotFoundError(rid.page_no, rid.slot_no);
    }
    memcpy(record->data, pageHandle.get_slot(rid.slot_no), file_hdr_.record_size);
    record->size = file_hdr_.record_size;

    // 解S锁
    if (context->txn_->get_isolation_level() < IsolationLevel::READ_COMMITTED) {
        context->lock_mgr_->unlock(context->txn_, lockDataId);
    }
    return record;
}

/**
 * @description: 在当前表中插入一条记录，不指定插入位置
 * @param {char*} buf 要插入的记录的数据
 * @param {Context*} context
 * @return {Rid} 插入的记录的记录号（位置）
 */
Rid RmFileHandle::insert_record(char* buf, Context* context) {
    RmPageHandle pageHandle = create_page_handle();
    int freeSlot = Bitmap::first_bit(false, pageHandle.bitmap, file_hdr_.num_records_per_page);
    auto rid = Rid{pageHandle.page->get_page_id().page_no, freeSlot};

    // 上X锁
    context->lock_mgr_->lock_exclusive_on_record(context->txn_, rid, fd_);
    auto lockDataId = LockDataId(fd_, rid, LockDataType::RECORD);

    memcpy(pageHandle.get_slot(freeSlot), buf, file_hdr_.record_size);
    Bitmap::set(pageHandle.bitmap, freeSlot);
    if (++pageHandle.page_hdr->num_records == file_hdr_.num_records_per_page) {
        file_hdr_.first_free_page_no = pageHandle.page_hdr->next_free_page_no;
    }

    // 解X锁
    if (context->txn_->get_isolation_level() < IsolationLevel::READ_COMMITTED) {
        context->lock_mgr_->unlock(context->txn_, lockDataId);
    }
    return rid;
}

/**
 * @description: 删除记录文件中记录号为rid的记录
 * @param {Rid&} rid 要删除的记录的记录号（位置）
 * @param {Context*} context
 */
void RmFileHandle::delete_record(const Rid& rid, Context* context) {
    // 上X锁
    context->lock_mgr_->lock_exclusive_on_record(context->txn_, rid, fd_);
    auto lockDataId = LockDataId(fd_, rid, LockDataType::RECORD);

    RmPageHandle pageHandle = fetch_page_handle(rid.page_no);
    if (!Bitmap::is_set(pageHandle.bitmap, rid.slot_no)) {
        throw PageNotExistError("", rid.page_no);
    }
    Bitmap::reset(pageHandle.bitmap, rid.slot_no);
    if (pageHandle.page_hdr->num_records == file_hdr_.num_records_per_page) {
        pageHandle.page_hdr->num_records--;
    } else {
        return;
    }
    release_page_handle(pageHandle);

    // 解X锁
    if (context->txn_->get_isolation_level() < IsolationLevel::READ_COMMITTED) {
        context->lock_mgr_->unlock(context->txn_, lockDataId);
    }
}

/**
 * @description: 更新记录文件中记录号为rid的记录
 * @param {Rid&} rid 要更新的记录的记录号（位置）
 * @param {char*} buf 新记录的数据
 * @param {Context*} context
 */
void RmFileHandle::update_record(const Rid& rid, char* buf, Context* context) {
    // 上X锁
    context->lock_mgr_->lock_exclusive_on_record(context->txn_, rid, fd_);
    auto lockDataId = LockDataId(fd_, rid, LockDataType::RECORD);

    RmPageHandle pageHandle = fetch_page_handle(rid.page_no);
    // 一定要记得更新bitmap
    if (!Bitmap::is_set(pageHandle.bitmap, rid.slot_no)) {
        throw PageNotExistError("", rid.page_no);
    }
    memcpy(pageHandle.get_slot(rid.slot_no), buf, file_hdr_.record_size);

    // 解X锁
    if (context->txn_->get_isolation_level() < IsolationLevel::READ_COMMITTED) {
        context->lock_mgr_->unlock(context->txn_, lockDataId);
    }
}

/**
 * 以下函数为辅助函数，仅提供参考，可以选择完成如下函数，也可以删除如下函数，在单元测试中不涉及如下函数接口的直接调用
 */
/**
 * @description: 获取指定页面的页面句柄
 * @param {int} page_no 页面号
 * @return {RmPageHandle} 指定页面的句柄
 */
RmPageHandle RmFileHandle::fetch_page_handle(int page_no) const {
    // Todo:
    // 使用缓冲池获取指定页面，并生成page_handle返回给上层
    // if page_no is invalid, throw PageNotExistError exception
    if (page_no >= file_hdr_.num_pages) {
        throw PageNotExistError("", page_no);
    }
    return RmPageHandle(
        &file_hdr_, buffer_pool_manager_->fetch_page({fd_, page_no}));
}

/**
 * @description: 创建一个新的page handle
 * @return {RmPageHandle} 新的PageHandle
 */
RmPageHandle RmFileHandle::create_new_page_handle() {
    PageId pageId = {fd_, INVALID_PAGE_ID};
    Page* newPage = buffer_pool_manager_->new_page(&pageId);
    auto pageHangle = RmPageHandle(&file_hdr_, newPage);
    if (newPage != nullptr) {
        RmPageHandle page_handle(&file_hdr_, newPage);
        pageHangle.page_hdr->num_records = 0;
        pageHangle.page_hdr->next_free_page_no = RM_NO_PAGE;
        file_hdr_.num_pages++;
        file_hdr_.first_free_page_no = pageId.page_no;
    }
    return pageHangle;
}

/**
 * @brief 创建或获取一个空闲的page handle
 *
 * @return RmPageHandle 返回生成的空闲page handle
 * @note pin the page, remember to unpin it outside!
 */
RmPageHandle RmFileHandle::create_page_handle() {
    // Todo:
    // 1. 判断file_hdr_中是否还有空闲页
    //     1.1 没有空闲页：使用缓冲池来创建一个新page；可直接调用create_new_page_handle()
    if (this->file_hdr_.first_free_page_no == RM_NO_PAGE) {
        return create_new_page_handle();
    }
    return fetch_page_handle(this->file_hdr_.first_free_page_no);
}

/**
 * @description: 当一个页面从没有空闲空间的状态变为有空闲空间状态时，更新文件头和页头中空闲页面相关的元数据
 */
void RmFileHandle::release_page_handle(RmPageHandle& page_handle) {
    page_handle.page_hdr->next_free_page_no = file_hdr_.first_free_page_no;
    file_hdr_.first_free_page_no = page_handle.page_hdr->next_free_page_no;
}
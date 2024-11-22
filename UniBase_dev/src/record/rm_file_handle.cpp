#include "rm_file_handle.h"

/**
 * @description: 获取当前表中记录号为rid的记录
 * @param {Rid&} rid 记录号，指定记录的位置
 * @param {Context*} context
 * @return {unique_ptr<RmRecord>} rid对应的记录对象指针
 */
std::unique_ptr<RmRecord> RmFileHandle::get_record(const Rid& rid, Context* context) const {
    // Transaction* txn = context->txn_;
    // 1. 获取指定记录所在的page handle
    RmPageHandle pageHandle = fetch_page_handle(rid.page_no);
    // context->lock_mgr_->lock_shared_on_record(txn, rid, pageHandle.page->get_page_id().fd);
    // 2. 初始化一个指向RmRecord的指针（赋值其内部的data和size）
    std::unique_ptr<RmRecord> record = std::make_unique<RmRecord>(
        (pageHandle.page_hdr->num_records * pageHandle.file_hdr->record_size), pageHandle.slots);
    return record;
}

/**
 * @description: 在当前表中插入一条记录，不指定插入位置
 * @param {char*} buf 要插入的记录的数据
 * @param {Context*} context
 * @return {Rid} 插入的记录的记录号（位置）
 */
Rid RmFileHandle::insert_record(char* buf, Context* context) {
    // 1. 获取当前未满的page handle
    RmPageHandle pageHandle = fetch_page_handle(file_hdr_.first_free_page_no);
    // 2. 在page handle中找到空闲slot位置
    char* freeSlot = pageHandle.get_slot(pageHandle.page_hdr->num_records);
    // 3. 将buf复制到空闲slot位置
    strcpy(freeSlot, buf);
    // 4. 更新page_handle.page_hdr中的数据结构
    pageHandle.page_hdr->num_records++;
    // 注意考虑插入一条记录后页面已满的情况，需要更新file_hdr_.first_free_page_no
    if (pageHandle.page_hdr->num_records >= file_hdr_.num_records_per_page) {
        file_hdr_.first_free_page_no++;
    }  // ljh | TODO: 需要根据后续写的东西判定一下这里是否需要更新每个页面的next_free_page_no
    return Rid{pageHandle.page->get_page_id().page_no, pageHandle.page_hdr->num_records - 1};
}

/**
 * @description: 删除记录文件中记录号为rid的记录
 * @param {Rid&} rid 要删除的记录的记录号（位置）
 * @param {Context*} context
 */
void RmFileHandle::delete_record(const Rid& rid, Context* context) {
    // Todo:
    // 1. 获取指定记录所在的page handle
    RmPageHandle pageHandle = fetch_page_handle(rid.page_no);
    // 2. 更新page_handle.page_hdr中的数据结构
    pageHandle.page_hdr->num_records--;
    pageHandle.bitmap[rid.slot_no] = 0;
    // 注意考虑删除一条记录后页面未满的情况，需要调用release_page_handle()
    release_page_handle(pageHandle);
}

/**
 * @description: 更新记录文件中记录号为rid的记录
 * @param {Rid&} rid 要更新的记录的记录号（位置）
 * @param {char*} buf 新记录的数据
 * @param {Context*} context
 */
void RmFileHandle::update_record(const Rid& rid, char* buf, Context* context) {
    // Todo:
    // 1. 获取指定记录所在的page handle
    RmPageHandle pageHandle = fetch_page_handle(rid.page_no);
    // 2. 更新记录
    char* destSlot = pageHandle.get_slot(rid.slot_no);
    strcpy(destSlot, buf);
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
    if (page_no == INVALID_PAGE_ID) {
        throw PageNotExistError("", page_no);
    }
    PageId tmpPageId = {fd_, page_no};
    Page* page = buffer_pool_manager_->fetch_page(tmpPageId);
    return RmPageHandle(&file_hdr_, page);
    return RmPageHandle(&file_hdr_, nullptr);
}

/**
 * @description: 创建一个新的page handle
 * @return {RmPageHandle} 新的PageHandle
 */
RmPageHandle RmFileHandle::create_new_page_handle() {
    // Todo:
    // 1.使用缓冲池来创建一个新page
    PageId* pageId;
    Page* page = buffer_pool_manager_->new_page(pageId);
    // 2.更新page handle中的相关信息
    RmPageHandle pageHandle = fetch_page_handle(pageId->page_no);
    // 3.更新file_hdr_
    file_hdr_.num_pages++;
    return RmPageHandle(&file_hdr_, page);
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
    if (file_hdr_.first_free_page_no == -1) {
        return create_new_page_handle();
    }
    //     1.2 有空闲页：直接获取第一个空闲页
    else {
        return fetch_page_handle(file_hdr_.first_free_page_no);
    }
    // 2. 生成page handle并返回给上层
    return RmPageHandle(&file_hdr_, nullptr);
}

/**
 * @description: 当一个页面从没有空闲空间的状态变为有空闲空间状态时，更新文件头和页头中空闲页面相关的元数据
 */
void RmFileHandle::release_page_handle(RmPageHandle &page_handle) {
    page_id_t pageNo = page_handle.page->get_page_id().page_no;
    page_handle.page_hdr->next_free_page_no = pageNo;
    if (file_hdr_.first_free_page_no > pageNo) {
        file_hdr_.first_free_page_no = pageNo;
    }
}
#include "buffer_pool_manager.h"
#include <iostream>
/**
 * @description: 从free_list或replacer中得到可淘汰帧页的 *frame_id
 * @return {bool} true: 可替换帧查找成功 , false: 可替换帧查找失败
 * @param {frame_id_t*} frame_id 帧页id指针,返回成功找到的可替换帧id
 */
bool BufferPoolManager::find_victim_page(frame_id_t *frame_id) {
    if (this->free_list_.empty()) {
        if (!this->replacer_->victim(frame_id)) {  // 空闲帧不足,调用LRU淘汰
            return false;                          // 淘汰失败
        }
    } else {
        *frame_id = this->free_list_.front();  // 还有空闲帧,直接使用
        this->free_list_.pop_front();
    }
    return true;
}

/**
 * @description: 更新页面数据, 如果为脏页则需写入磁盘，再更新为新页面，更新page元数据(data, is_dirty, page_id)和page table
 * @param {Page*} page 写回页指针
 * @param {PageId} new_page_id 新的page_id
 * @param {frame_id_t} new_frame_id 新的帧frame_id
 */
void BufferPoolManager::update_page(Page *page, PageId new_page_id, frame_id_t new_frame_id) {
    if(page->is_dirty()) {  //脏位处理
        this->disk_manager_->write_page(
            page->get_page_id().fd, page->get_page_id().page_no, page->get_data(), PAGE_SIZE);
        page->is_dirty_ = false;
    }

    page->reset_memory();

    for(auto position = this->page_table_.begin(); position != this->page_table_.end(); position++) {  //更新table
        if(position->first == page->id_) {
            this->page_table_.erase(position);
            break;
        }
    }
    this->page_table_[new_page_id] = new_frame_id;

    page->id_ = new_page_id;
    if(page->id_.page_no != INVALID_PAGE_ID) {
        this->disk_manager_->read_page(
            page->get_page_id().fd, page->get_page_id().page_no, page->get_data(), PAGE_SIZE);
    }
}

/**
 * @description: 从buffer pool获取需要的页。
 *              如果页表中存在page_id（说明该page在缓冲池中），并且pin_count++。
 *              如果页表不存在page_id（说明该page在磁盘中），则找缓冲池victim page，将其替换为磁盘中读取的page，pin_count置1。
 * @return {Page*} 若获得了需要的页则将其返回，否则返回nullptr
 * @param {PageId} page_id 需要获取的页的PageId
 */
Page *BufferPoolManager::fetch_page(PageId page_id) {
    // Todo:
    //  1.     从page_table_中搜寻目标页
    std::scoped_lock lock{latch_};
    frame_id_t id;
    int flag = 0;
    if (this->page_table_.find(page_id) != this->page_table_.end()) {  // 是否在缓冲池
        id = this->page_table_[page_id];
        flag = 1;
    } else {
        if (!this->find_victim_page(&id)) {  // 找空闲帧或替换
            return nullptr;
        }
        this->update_page(&this->pages_[id], page_id, id);
    }

    this->replacer_->pin(id);
    if (flag == 1) {
        this->pages_[id].pin_count_++;
    } else {
        this->pages_[id].pin_count_ = 1;
    }

    return &this->pages_[id];
}

/**
 * @description: 取消固定pin_count>0的在缓冲池中的page
 * @return {bool} 如果目标页的pin_count<=0则返回false，否则返回true
 * @param {PageId} page_id 目标page的page_id
 * @param {bool} is_dirty 若目标page应该被标记为dirty则为true，否则为false
 */
bool BufferPoolManager::unpin_page(PageId page_id, bool is_dirty) {
    std::scoped_lock lock{latch_};
    // for(auto it : page_table_){
    //     std::cout << it.first.page_no << " ";
    // }
    // std::cout << std::endl;
    if (this->page_table_.find(page_id) == this->page_table_.end()) {
        return false;
    }
    frame_id_t id = this->page_table_[page_id];
    Page *page = &this->pages_[id];
    if (page->pin_count_ <= 0) {
        return false;
    }
    page->pin_count_--;
    if (page->pin_count_ == 0) {
        this->replacer_->unpin(id);
    }
    if (is_dirty) {
        page->is_dirty_ = true;
    }
    return true; 
}

/**
 * @description: 将目标页写回磁盘，不考虑当前页面是否正在被使用
 * @return {bool} 成功则返回true，否则返回false(只有page_table_中没有目标页时)
 * @param {PageId} page_id 目标页的page_id，不能为INVALID_PAGE_ID
 */
bool BufferPoolManager::flush_page(PageId page_id) {
    std::scoped_lock lock{latch_};

    if (this->page_table_.find(page_id) == this->page_table_.end()) {
        return false;
    }
    frame_id_t id = this->page_table_[page_id];  // 获取id
    Page *page = &this->pages_[id];              // 通过id获取page

    this->disk_manager_->write_page(
        page->get_page_id().fd, page->get_page_id().page_no, page->get_data(), PAGE_SIZE);
    page->is_dirty_ = false;
    return true;
}

/**
 * @description: 创建一个新的page，即从磁盘中移动一个新建的空page到缓冲池某个位置。
 * @return {Page*} 返回新创建的page，若创建失败则返回nullptr
 * @param {PageId*} page_id 当成功创建一个新的page时存储其page_id
 */
Page *BufferPoolManager::new_page(PageId *page_id) {
    std::scoped_lock lock{latch_};

    frame_id_t id;
    if(this->find_victim_page(&id)) {  //找到一个位置
        page_id->page_no = this->disk_manager_->allocate_page(page_id->fd); //获取编号
        this->update_page(&this->pages_[id], *page_id, id);  //更新page
        this->replacer_->pin(id);
        this->pages_[id].pin_count_ = 1;

    } else {
        return nullptr;
    }
    return &this->pages_[id];
}

/**
 * @description: 从buffer_pool删除目标页
 * @return {bool} 如果目标页不存在于buffer_pool或者成功被删除则返回true，若其存在于buffer_pool但无法删除则返回false
 * @param {PageId} page_id 目标页
 */
bool BufferPoolManager::delete_page(PageId page_id) {
    std::scoped_lock lock{latch_};

    if (this->page_table_.find(page_id) == this->page_table_.end()) {
        return true;
    }
    frame_id_t id = this->page_table_[page_id];
    Page *page = &this->pages_[id];
    if (page->pin_count_ != 0) {  // 还在被使用，不能删除
        return false;
    }
    this->disk_manager_->deallocate_page(page->get_page_id().page_no);
    page_id.page_no = INVALID_PAGE_ID;
    this->update_page(page, page_id, id);  // 包含page table处理
    this->free_list_.push_back(id);
    return true;
}

/**
 * @description: 将buffer_pool中的所有页写回到磁盘
 * @param {int} fd 文件句柄
 */
void BufferPoolManager::flush_all_pages(int fd) {
    std::scoped_lock lock{latch_};
    for (size_t i = 0; i < pool_size_; i++) {
        Page *page = &this->pages_[i];
        if (page->get_page_id().fd == fd && page->get_page_id().page_no != INVALID_PAGE_ID) {
            disk_manager_->write_page(page->get_page_id().fd, page->get_page_id().page_no, page->get_data(), PAGE_SIZE);
            page->is_dirty_ = false;
        }
    }
}
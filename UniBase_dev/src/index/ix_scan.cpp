#include "ix_scan.h"

/**
 * @brief 
 * @todo 加上读锁（需要使用缓冲池得到page）
 */
void IxScan::next() { //这个函数的目标是移到索引中的下一个记录
    assert(!is_end());//检查是否到了索引的末尾
    IxNodeHandle *node = ih_->fetch_node(iid_.page_no);//从索引处理器中获取当前页。
    //这个操作会调用缓冲池（Buffer Pool）中的相关功能，通常用于减少磁盘访问，提高性能。
    assert(node->is_leaf_page());//确认所获取的节点是否为叶子节点。
    //叶子节点通常包含实际的数据或记录标识符。
    assert(iid_.slot_no < node->get_size());
    // increment slot no
    iid_.slot_no++;
    //准备访问当前页的下一个记录
    if (iid_.page_no != ih_->file_hdr_->last_leaf_ && iid_.slot_no == node->get_size()) {
        // go to next leaf
        iid_.slot_no = 0;
        iid_.page_no = node->get_next_leaf();
    }
}

Rid IxScan::rid() const {
    return ih_->get_rid(iid_);
}
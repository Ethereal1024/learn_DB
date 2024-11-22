#include "rm_scan.h"

#include "rm_file_handle.h"

/**
 * @brief 初始化file_handle和rid
 * @param file_handle
 */
RmScan::RmScan(const RmFileHandle *file_handle) : file_handle_(file_handle) {
    // Todo:
    // 初始化file_handle和rid（指向第一个存放了记录的位置）
    int page_no = 0, slot_no = 0;
    while (!file_handle_->is_record(Rid{page_no, slot_no})) {
        if (slot_no == file_handle_->file_hdr_.num_records_per_page - 1) {
            page_no++;
            slot_no = 0;
        }
        slot_no++;
    }
    rid_ = Rid{page_no, slot_no};
}

/**
 * @brief 找到文件中下一个存放了记录的位置
 */
void RmScan::next() {
    // Todo:
    // 找到文件中下一个存放了记录的非空闲位置，用rid_来指向这个位置
    int page_no = rid_.page_no, slot_no = rid_.slot_no + 1;
    while (!file_handle_->is_record(Rid{page_no, slot_no})) {
        if (slot_no == file_handle_->file_hdr_.num_records_per_page - 1) {
            page_no++;
            slot_no = 0;
        }
        slot_no++;
    }
    rid_ = Rid{page_no, slot_no};
}

/**
 * @brief ​ 判断是否到达文件末尾
 */
bool RmScan::is_end() const {
    // Todo: 修改返回值
    bool endPage = rid_.page_no == file_handle_->file_hdr_.num_pages;
    bool endSlot = rid_.slot_no == file_handle_->file_hdr_.num_records_per_page - 1;
    if (endPage && endSlot) {
        return true;
    }
    return false;
}

/**
 * @brief RmScan内部存放的rid
 */
Rid RmScan::rid() const {
    return rid_;
}
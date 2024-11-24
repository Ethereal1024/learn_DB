#include "rm_scan.h"

#include "rm_file_handle.h"

/**
 * @brief 初始化file_handle和rid
 * @param file_handle
 */
RmScan::RmScan(const RmFileHandle *file_handle) : file_handle_(file_handle) {
    // Todo:
    // 初始化file_handle和rid（指向第一个存放了记录的位置）
    // 初始化file_handle_
    rid_ = {.page_no = RM_FIRST_RECORD_PAGE, .slot_no = -1};
    next();
}

/**
 * @brief 找到文件中下一个存放了记录的位置
 */
void RmScan::next() {
    // Todo:
    // 找到文件中下一个存放了记录的非空闲位置，用rid_来指向这个位置
    while (rid_.page_no < file_handle_->file_hdr_.num_pages) {
        RmPageHandle pageHandle = file_handle_->fetch_page_handle(rid_.page_no);
        rid_.slot_no = Bitmap::next_bit(
            true, pageHandle.bitmap,
            file_handle_->file_hdr_.num_records_per_page,
            rid_.slot_no);
        if (rid_.slot_no < this->file_handle_->file_hdr_.num_records_per_page) {
            return;
        } else {
            rid_ = Rid{rid_.page_no + 1, -1};
            if (rid_.page_no >= file_handle_->file_hdr_.num_pages) {
                rid_ = Rid{RM_NO_PAGE, -1};
                break;
            }
        }
    }
}

/**
 * @brief ​ 判断是否到达文件末尾
 */
bool RmScan::is_end() const {
    // Todo: 修改返回值
    return rid_.page_no == RM_NO_PAGE;
}

/**
 * @brief RmScan内部存放的rid
 */
Rid RmScan::rid() const {
    return rid_;
}
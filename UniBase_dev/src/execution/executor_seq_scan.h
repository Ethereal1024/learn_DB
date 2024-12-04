#pragma once

#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class SeqScanExecutor : public AbstractExecutor {
private:
    std::string tab_name_;
    std::vector<Condition> conds_;
    RmFileHandle *fh_;
    std::vector<ColMeta> cols_;
    size_t len_;
    std::vector<Condition> fed_conds_;
    Rid rid_;
    std::unique_ptr<RecScan> scan_;
    SmManager *sm_manager_;

public:
    SeqScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = std::move(tab_name);
        conds_ = std::move(conds);
        TabMeta &tab = sm_manager_->db_.get_table(tab_name_);
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        cols_ = tab.cols;
        len_ = cols_.back().offset + cols_.back().len;
        context_ = context;
        fed_conds_ = conds_;
    }

    void beginTuple() override {
        scan_ = std::make_unique<RmScan>(fh_);
        while (!scan_->is_end()) {
            rid_ = scan_->rid();
            try {
                auto rec = fh_->get_record(rid_, context_);
                if (eval_conds(rec.get())) {
                    return;
                }
            } catch (RecordNotFoundError &e) {
                std::cerr << e.what() << std::endl;
            }
            scan_->next();
        }
    }

    void nextTuple() override {
        while (!scan_->is_end()) {
            scan_->next();
            rid_ = scan_->rid();
            try {
                auto rec = fh_->get_record(rid_, context_);
                if (eval_conds(rec.get())) {
                    return;
                }
            } catch (RecordNotFoundError &e) {
                std::cerr << e.what() << std::endl;
            }
        }
    }

    std::unique_ptr<RmRecord> Next() override {
        if (scan_->is_end()) {
            return nullptr;
        }
        auto rec = fh_->get_record(rid_, context_);
        nextTuple();
        return rec;
    }

    void updateFeed(const std::map<TabCol, Value> &feed_dict) {
        fed_conds_.clear();
        for (auto &cond : conds_) {
            Condition new_cond = cond;
            if (feed_dict.find(cond.lhs_col) != feed_dict.end()) {
                new_cond.rhs_val = feed_dict.at(cond.lhs_col);
                new_cond.is_rhs_val = true;
            }
            fed_conds_.push_back(new_cond);
        }
    }

    Rid &rid() override { return rid_; }

private:
    bool eval_conds(const RmRecord *record) {
        for (const auto &cond : fed_conds_) {
            Value lhs_value = get_value(record, cond.lhs_col);
            Value rhs_value = cond.is_rhs_val ? cond.rhs_val : get_value(record, cond.rhs_col);
            if (!compare(lhs_value, rhs_value, cond.op)) {
                return false;
            }
        }
        return true;
    }

    Value get_value(const RmRecord *record, const TabCol &col) {
        // 获取列元数据
        auto col_meta = std::find_if(cols_.begin(), cols_.end(), [&](const ColMeta &meta) { return meta.tab_name == col.tab_name && meta.name == col.col_name; });
        // 检查是否找到列
        if (col_meta == cols_.end()) {
            throw std::runtime_error("Column not found");
        }
        Value value;
        memcpy(&value.val, record->data + col_meta->offset, sizeof(int));
        return value;
    }

    bool compare(const Value &lhs, const Value &rhs, CompOp op) {
        // 使用 Value 类中的比较方法进行比较
        switch (op) {
        case OP_EQ:
            return lhs.equals(rhs);
        case OP_NE:
            return lhs.not_equals(rhs);
        case OP_LT:
            return lhs.less_than(rhs);
        case OP_LE:
            return lhs.less_than_equal(rhs);
        case OP_GT:
            return lhs.greater_than(rhs);
        case OP_GE:
            return lhs.greater_than_equal(rhs);
        default:
            return false;
        }
    }
};

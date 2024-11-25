#pragma once

#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

// 定义比较操作符
enum CompOp
{
    OP_EQ, // Equal
    OP_NE, // Not equal
    OP_LT, // Less than
    OP_LE, // Less than or equal to
    OP_GT, // Greater than
    OP_GE  // Greater than or equal to
};

// 示例 Value 类，假设包含一个简单的 int 值
class Value
{
public:
    int val;

    bool equals(const Value &other) const
    {
        return val == other.val;
    }

    bool not_equals(const Value &other) const
    {
        return val != other.val;
    }

    bool less_than(const Value &other) const
    {
        return val < other.val;
    }

    bool less_than_equal(const Value &other) const
    {
        return val <= other.val;
    }

    bool greater_than(const Value &other) const
    {
        return val > other.val;
    }

    bool greater_than_equal(const Value &other) const
    {
        return val >= other.val;
    }
};

class IndexScanExecutor : public AbstractExecutor
{
private:
    std::string tab_name_;             // 表名称
    TabMeta tab_;                      // 表的元数据
    std::vector<Condition> conds_;     // 扫描条件
    RmFileHandle *fh_;                 // 表的数据文件句柄
    std::vector<ColMeta> cols_;        // 需要读取的字段
    size_t len_;                       // 选取出来的一条记录的长度
    std::vector<Condition> fed_conds_; // 实际的扫描条件

    std::vector<std::string> index_col_names_; // 索引包含的字段
    IndexMeta index_meta_;                     // 索引的元数据

    Rid rid_;
    std::unique_ptr<RecScan> scan_;
    SmManager *sm_manager_;
    BufferPoolManager *bpm_;  // 缓冲池管理器
    const IxIndexHandle *ih_; // 索引句柄

public:
    IndexScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds,
                      std::vector<std::string> index_col_names, Context *context,
                      BufferPoolManager *bpm, const IxIndexHandle *ih)
        : bpm_(bpm), ih_(ih)
    {
        sm_manager_ = sm_manager;
        context_ = context;
        tab_name_ = std::move(tab_name);
        tab_ = sm_manager_->db_.get_table(tab_name_);
        conds_ = std::move(conds);
        index_col_names_ = std::move(index_col_names);
        index_meta_ = *(tab_.get_index_meta(index_col_names_));
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        cols_ = tab_.cols;
        len_ = cols_.back().offset + cols_.back().len;

        // 处理扫描条件，确保连接运算的条件谓词正确
        std::map<CompOp, CompOp> swap_op = {
            {OP_EQ, OP_EQ}, {OP_NE, OP_NE}, {OP_LT, OP_GT}, {OP_GT, OP_LT}, {OP_LE, OP_GE}, {OP_GE, OP_LE}};

        for (auto &cond : conds_)
        {
            if (cond.lhs_col.tab_name != tab_name_)
            {
                assert(!cond.is_rhs_val && cond.rhs_col.tab_name == tab_name_);
                std::swap(cond.lhs_col, cond.rhs_col);
                cond.op = swap_op.at(cond.op);
            }
        }
        fed_conds_ = conds_;
    }

    void beginTuple() override
    {
        // 初始化索引扫描
        Iid lower = ih_->leaf_begin();
        Iid upper = ih_->leaf_end();

        // 根据条件调整 lower 和 upper
        adjust_bounds(lower, upper);

        scan_ = std::make_unique<IxScan>(ih_, lower, upper, bpm_);

        // 找到第一个满足谓词条件的元组
        nextTuple();
    }

    void nextTuple() override
    {
        while (!scan_->is_end())
        {
            scan_->next();
            rid_ = scan_->rid();
            try
            {
                auto rec = fh_->get_record(rid_, context_);
                if (eval_conds(rec.get()))
                {
                    return; // 满足谓词条件，停止扫描
                }
            }
            catch (RecordNotFoundError &e)
            {
                std::cerr << e.what() << std::endl;
            }
        }
    }

    std::unique_ptr<RmRecord> Next() override
    {
        if (scan_->is_end())
        {
            return nullptr; // 如果扫描结束，返回空指针
        }
        auto rec = fh_->get_record(rid_, context_);
        nextTuple(); // 准备下一个满足谓词条件的记录
        return rec;  // 返回当前满足谓词条件的记录
    }

    void updateFeed(const std::map<TabCol, Value> &feed_dict)
    {
        fed_conds_.clear();
        for (auto &cond : conds_)
        {
            Condition new_cond = cond;
            if (feed_dict.find(cond.lhs_col) != feed_dict.end())
            {
                new_cond.rhs_val = feed_dict.at(cond.lhs_col);
                new_cond.is_rhs_val = true;
            }
            fed_conds_.push_back(new_cond);
        }
    }

    Rid &rid() override { return rid_; }

private:
    bool eval_conds(const RmRecord *record)
    {
        for (const auto &cond : fed_conds_)
        {
            // 获取记录中左列和右值（或右列）的值
            Value lhs_value = get_value(record, cond.lhs_col);
            Value rhs_value = cond.is_rhs_val ? cond.rhs_val : get_value(record, cond.rhs_col);

            // 判断条件是否满足
            if (!compare(lhs_value, rhs_value, cond.op))
            {
                return false; // 如果有一个条件不满足，返回false
            }
        }
        return true; // 所有条件都满足，返回true
    }

    Value get_value(const RmRecord *record, const TabCol &col)
    {
        // 获取列元数据
        auto col_meta = std::find_if(cols_.begin(), cols_.end(), [&](const ColMeta &meta)
                                     { return meta.tab_name == col.tab_name && meta.name == col.col_name; });
        if (col_meta == cols_.end())
        {
            throw std::runtime_error("Column not found");
        }
        Value value;
        memcpy(&value.val, record->data + col_meta->offset, sizeof(int));
        return value;
    }

    bool compare(const Value &lhs, const Value &rhs, CompOp op)
    {
        // 使用 Value 类中的比较方法进行比较
        switch (op)
        {
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

    void adjust_bounds(Iid &lower, Iid &upper)
    {
        // 根据条件调整 lower 和 upper 的逻辑
        for (const auto &cond : fed_conds_)
        {
            // 示例逻辑，可以根据具体情况调整
            if (cond.op == OP_LT || cond.op == OP_LE)
            {
                // 调整 upper
                // 示例：upper = ih_->find_upper_bound(cond.value);
            }
            else if (cond.op == OP_GT || cond.op == OP_GE)
            {
                // 调整 lower
                // 示例：lower = ih_->find_lower_bound(cond.value);
            }
        }
    }
};

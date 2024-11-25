#include "sm_manager.h"

#include <sys/stat.h>
#include <unistd.h>

#include <fstream>

#include "index/ix.h"
#include "record/rm.h"
#include "record_printer.h"

/**
 * @description: 判断是否为一个文件夹
 * @return {bool} 返回是否为一个文件夹
 * @param {string&} db_name 数据库文件名称，与文件夹同名
 */
bool SmManager::is_dir(const std::string &db_name)
{
    struct stat st;
    return stat(db_name.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

/**
 * @description: 创建数据库，所有的数据库相关文件都放在数据库同名文件夹下
 * @param {string&} db_name 数据库名称
 */
void SmManager::create_db(const std::string &db_name)
{
    if (is_dir(db_name))
    {
        throw DatabaseExistsError(db_name);
    }
    // 为数据库创建一个子目录
    std::string cmd = "mkdir " + db_name;
    if (system(cmd.c_str()) < 0)
    { // 创建一个名为db_name的目录
        throw UnixError();
    }
    if (chdir(db_name.c_str()) < 0)
    { // 进入名为db_name的目录
        throw UnixError();
    }
    // 创建系统目录
    DbMeta *new_db = new DbMeta();
    new_db->name_ = db_name;

    // 注意，此处ofstream会在当前目录创建(如果没有此文件先创建)和打开一个名为DB_META_NAME的文件
    std::ofstream ofs(DB_META_NAME);

    // 将new_db中的信息，按照定义好的operator<<操作符，写入到ofs打开的DB_META_NAME文件中
    ofs << *new_db; // 注意：此处重载了操作符<<

    delete new_db;

    // 创建日志文件
    disk_manager_->create_file(LOG_FILE_NAME);

    // 回到根目录
    if (chdir("..") < 0)
    {
        throw UnixError();
    }
}

/**
 * @description: 删除数据库，同时需要清空相关文件以及数据库同名文件夹
 * @param {string&} db_name 数据库名称，与文件夹同名
 */
void SmManager::drop_db(const std::string &db_name)
{
    if (!is_dir(db_name))
    {
        throw DatabaseNotFoundError(db_name);
    }
    std::string cmd = "rm -r " + db_name;
    if (system(cmd.c_str()) < 0)
    {
        throw UnixError();
    }
}

/**
 * @description: 打开数据库，找到数据库对应的文件夹，并加载数据库元数据和相关文件
 * @param {string&} db_name 数据库名称，与文件夹同名
 */
void SmManager::open_db(const std::string &db_name)
{
    if (!is_dir(db_name))
    {
        throw DatabaseNotFoundError(db_name);
    }
    if (chdir(db_name.c_str()) < 0)
    {
        throw UnixError();
    }
    std::ifstream ifs(DB_META_NAME);
    ifs >> db_;
    for (auto &entry : db_.tabs_)
    {
        auto &tab = entry.second;
        fhs_.emplace(tab.name, rm_manager_->open_file(tab.name));
        for (size_t i = 0; i < tab.cols.size(); i++)
        {
            auto &col = tab.cols[i];
            if (col.index)
            {
                std::vector<ColMeta> index_cols = {col};
                auto index_name = ix_manager_->get_index_name(tab.name, index_cols);
                assert(ihs_.count(index_name) == 0);
                ihs_.emplace(index_name, ix_manager_->open_index(tab.name, index_cols));
            }
        }
    }
}

/**
 * @description: 把数据库相关的元数据刷入磁盘中
 */
void SmManager::flush_meta()
{
    // 默认清空文件
    std::ofstream ofs(DB_META_NAME);
    ofs << db_;
}

/**
 * @description: 关闭数据库并把数据落盘
 */
#include <fstream>

void SmManager::close_db()
{
    if (db_.name_.empty())
    {
        std::cerr << "No database is currently open!" << std::endl;
        return;
    }

    std::string meta_file = db_.name_ + ".meta";

    try
    {

        std::ofstream ofs(meta_file, std::ios::out | std::ios::trunc);
        if (!ofs.is_open())
        {
            throw std::ios_base::failure("Failed to open metadata file for writing: " + meta_file);
        }

        ofs << db_;
        ofs.close();
        std::cout << "Database metadata saved to " << meta_file << std::endl;

        fhs_.clear();

        ihs_.clear();

        db_ = DbMeta();
        std::cout << "Database closed successfully." << std::endl;
    }
    catch (const std::exception &e)
    {
        std::cerr << "Error while closing database: " << e.what() << std::endl;
        throw;
    }
}

/**
 * @description: 显示所有的表,通过测试需要将其结果写入到output.txt,详情看题目文档
 * @param {Context*} context
 */
void SmManager::show_tables(Context *context)
{
    std::fstream outfile;
    outfile.open("output.txt", std::ios::out | std::ios::app);
    outfile << "| Tables |\n";
    RecordPrinter printer(1);
    printer.print_separator(context);
    printer.print_record({"Tables"}, context);
    printer.print_separator(context);
    for (auto &entry : db_.tabs_)
    {
        auto &tab = entry.second;
        printer.print_record({tab.name}, context);
        outfile << "| " << tab.name << " |\n";
    }
    printer.print_separator(context);
    outfile.close();
}

/**
 * @description: 显示表的元数据
 * @param {string&} tab_name 表名称
 * @param {Context*} context
 */
void SmManager::desc_table(const std::string &tab_name, Context *context)
{
    TabMeta &tab = db_.get_table(tab_name);

    std::vector<std::string> captions = {"Field", "Type", "Index"};
    RecordPrinter printer(captions.size());
    // Print header
    printer.print_separator(context);
    printer.print_record(captions, context);
    printer.print_separator(context);
    // Print fields
    for (auto &col : tab.cols)
    {
        std::vector<std::string> field_info = {col.name, coltype2str(col.type), col.index ? "YES" : "NO"};
        printer.print_record(field_info, context);
    }
    // Print footer
    printer.print_separator(context);
}

/**
 * @description: 创建表
 * @param {string&} tab_name 表的名称
 * @param {vector<ColDef>&} col_defs 表的字段
 * @param {Context*} context
 */
void SmManager::create_table(const std::string &tab_name, const std::vector<ColDef> &col_defs, Context *context)
{
    if (db_.is_table(tab_name))
    {
        throw TableExistsError(tab_name);
    }
    // Create table meta
    int curr_offset = 0;
    TabMeta tab;
    tab.name = tab_name;
    for (auto &col_def : col_defs)
    {
        ColMeta col = {.tab_name = tab_name,
                       .name = col_def.name,
                       .type = col_def.type,
                       .len = col_def.len,
                       .offset = curr_offset,
                       .index = false};
        curr_offset += col_def.len;
        tab.cols.push_back(col);
    }
    // Create & open record file
    int record_size = curr_offset; // record_size就是col meta所占的大小（表的元数据也是以记录的形式进行存储的）
    rm_manager_->create_file(tab_name, record_size);
    db_.tabs_[tab_name] = tab;
    // fhs_[tab_name] = rm_manager_->open_file(tab_name);
    fhs_.emplace(tab_name, rm_manager_->open_file(tab_name));

    flush_meta();
}

/**
 * @description: 删除表
 * @param {string&} tab_name 表的名称
 * @param {Context*} context
 */

void SmManager::drop_table(const std::string &tab_name, Context *context)
{

    if (!db_.is_table(tab_name))
    {
        throw TableNotFoundError(tab_name);
    }

    TabMeta &tab = db_.get_table(tab_name);

    for (auto &index_meta : tab.indexes)
    {
        std::vector<std::string> col_names;
        for (auto &col : index_meta.cols)
        {
            col_names.push_back(col.name);
        }
        drop_index(tab_name, col_names, context);
    }

    std::string data_file_name = tab.name;
    if (fhs_.count(data_file_name))
    {

        rm_manager_->close_file(fhs_[data_file_name].get());
        fhs_.erase(data_file_name);
    }
    disk_manager_->destroy_file(data_file_name);

    db_.tabs_.erase(tab_name);

    flush_meta();
}

/**
 * @description: 创建索引
 * @param {string&} tab_name 表的名称
 * @param {vector<string>&} col_names 索引包含的字段名称
 * @param {Context*} context
 */
void SmManager::create_index(const std::string &tab_name, const std::vector<std::string> &col_names, Context *context)
{

    TabMeta &tab = db_.get_table(tab_name);

    for (const auto &col_name : col_names)
    {
        auto col = tab.get_col(col_name);
        if (col->index)
        {
            throw IndexExistsError(tab_name, col_names);
        }
    }

    std::vector<ColMeta> index_cols;
    for (const auto &col_name : col_names)
    {
        auto col = tab.get_col(col_name);
        index_cols.push_back(*col);
    }
    ix_manager_->create_index(tab_name, index_cols);
    for (const auto &col_name : col_names)
    {
        auto col = tab.get_col(col_name);
        col->index = true;
    }

    flush_meta();
}

/**
 * @description: 删除索引
 * @param {string&} tab_name 表名称
 * @param {vector<string>&} col_names 索引包含的字段名称
 * @param {Context*} context
 */
void SmManager::drop_index(const std::string &tab_name, const std::vector<std::string> &col_names, Context *context)
{

    TabMeta &tab = db_.tabs_.at(tab_name);


    for (const auto &col_name : col_names)
    {

        auto col = tab.get_col(col_name);

        if (!col->index)
        {
            throw IndexNotFoundError(tab_name, col_names);
        }
        int col_idx = std::distance(tab.cols.begin(), col);
        auto index_name = ix_manager_->get_index_name(tab_name, std::vector<std::string>{col_name});
        ix_manager_->close_index(ihs_.at(index_name).get());

        ix_manager_->destroy_index(tab_name, std::vector<std::string>{col_name});
        ihs_.erase(index_name);
        col->index = false;
    }
    flush_meta();
}


void SmManager::rollback_insert(const std::string& tab_name, const Rid& rid, Context* context) {
    auto tab = db_.get_table(tab_name);
    auto record = fhs_.at(tab_name).get()->get_record(rid, context);
    for (auto index : tab.indexes) {
        IxIndexHandle* indexHandle = ihs_.at(get_ix_manager()->get_index_name(tab_name, index.cols)).get();
        for (auto column : index.cols) {
            indexHandle->delete_entry(record->data + column.offset, nullptr);
        }
    }
    fhs_.at(tab_name).get()->delete_record(rid, context);
}

void SmManager::rollback_delete(const std::string& tab_name, const RmRecord& record, Context* context) {
    auto tab = db_.get_table(tab_name);
    auto rid = fhs_.at(tab_name).get()->insert_record(record.data, context);
    for (auto index : tab.indexes) {
        IxIndexHandle* indexHandle = ihs_.at(get_ix_manager()->get_index_name(tab_name, index.cols)).get();
        for (auto column : index.cols) {
            indexHandle->insert_entry(record.data + column.offset, rid, context->txn_);
        }
    }
}

}

// void SmManager::rollback_update(const std::string &tab_name, const Rid &rid, const RmRecord &record, Context *context) {
//     auto tab = db_.get_table(tab_name);
//     auto rec = fhs_.at(tab_name).get()->get_record(rid, context);
//     // delete entry
//     for (size_t i=0; i<tab.cols.size(); i++){
//         if (tab.cols[i].index)
//             ihs_.at(get_ix_manager()->get_index_name(tab_name, i)).get()->delete_entry(rec->data+tab.cols[i].offset, nullptr);
//     }
//     // update record
//     fhs_.at(tab_name).get()->update_record(rid, record.data, context);
//     // insert entry
//     for (size_t i=0; i<tab.cols.size(); i++){
//         if (tab.cols[i].index)
//             ihs_.at(get_ix_manager()->get_index_name(tab_name, i)).get()->insert_entry(record.data+tab.cols[i].offset, rid, context->txn_);
//     }
// }
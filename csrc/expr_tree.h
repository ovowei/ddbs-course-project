#ifndef EXPR_TREE_H
#define EXPR_TREE_H

#include <memory>
#include <string>
#include <vector>

class Schema {
   public:
    int n_columns;
    std::vector<std::string> column_name;
};

// Expr tree node structure
class Expr {
   public:
    enum Type { EQUALS, NOTEQUALS, LIKE, NOTLIKE, AND, OR, TABLEVALUE, CONSTVALUE } type;

    // For non-leaf node
    std::shared_ptr<Expr> child1;
    std::shared_ptr<Expr> child2;

    // For leaf node (TABLEVALUE or CONSTVALUE)
    std::string column_name;
    std::string const_string;

    // Constructor for leaf nodes
    Expr(Type type, const std::string& column_name = "", const std::string& const_string = "")
        : type(type), column_name(column_name), const_string(const_string), child1(nullptr), child2(nullptr) {}

    // Constructor for non-leaf nodes
    Expr(Type type, std::shared_ptr<Expr> child1, std::shared_ptr<Expr> child2) : type(type), child1(child1), child2(child2) {}
};

// Common description for Join and Union, as they both have key pairs
struct KeyDescription {
    std::string key1;
    std::string key2;
};

// Aggregate type for Groupby
enum AggrType { COUNT, SUM, MAX, MIN, AVG };

// Plan tree node structure
class Treenode {
   public:
    enum Position { MYSQL1, MYSQL2, CONTROL } position;

    Schema* schema;
    enum Operation {
        NONE, 
        JOIN,
        UNION,
        GROUPBY,
        ORDERBY,
        LIMIT,
        FILTER,
        PROJECT
    } operation;

    // Description for operations
    std::shared_ptr<KeyDescription> joinDescription;
    std::shared_ptr<KeyDescription> unionDescription;  
    std::shared_ptr<GroupbyDescription> groupbyDescription;
    std::shared_ptr<OrderbyDescription> orderbyDescription;
    std::shared_ptr<LimitDescription> limitDescription;
    std::shared_ptr<FilterDescription> filterDescription;

    // Child nodes
    std::shared_ptr<Treenode> child1;
    std::shared_ptr<Treenode> child2;
    std::string subquery_sql;

    // Constructor for internal nodes (JOIN, GROUPBY, etc.)
    Treenode(Operation operation, std::shared_ptr<Treenode> child1 = nullptr, std::shared_ptr<Treenode> child2 = nullptr)
        : operation(operation), child1(child1), child2(child2), position(CONTROL), schema(nullptr) {
        // Initialize all other descriptions to nullptr
        unionDescription = nullptr;
        joinDescription = nullptr;
        groupbyDescription = nullptr;
        orderbyDescription = nullptr;
        limitDescription = nullptr;
        filterDescription = nullptr;
    }

    // Constructor for leaf nodes (MYSQL1, MYSQL2, CONTROL)
    Treenode(Position pos = CONTROL, Schema* sch = nullptr, Operation op = NONE) : position(pos), schema(sch), operation(op), child1(nullptr), child2(nullptr) {
        // Initialize all descriptions to nullptr
        unionDescription = nullptr;
        joinDescription = nullptr;
        groupbyDescription = nullptr;
        orderbyDescription = nullptr;
        limitDescription = nullptr;
        filterDescription = nullptr;
    }

    // Set the filter condition
    void setFilter(std::shared_ptr<Expr> condition) {
        filterDescription = std::make_shared<FilterDescription>();
        filterDescription->condition = condition;
    }
};

// Groupby description
class GroupbyDescription {
   public:
    std::vector<std::string> keys;
    std::vector<AggrType> aggr;

    GroupbyDescription(const std::vector<std::string>& keys, const std::vector<AggrType>& aggr) : keys(keys), aggr(aggr) {}
};

// Orderby description
class OrderbyDescription {
   public:
    std::vector<std::string> keys;
    enum OrderType { ASC, DESC } cmp;

    OrderbyDescription(const std::vector<std::string>& keys, OrderType cmp) : keys(keys), cmp(cmp) {}
};

// Limit description
class LimitDescription {
   public:
    int limit_num;

    LimitDescription(int limit_num) : limit_num(limit_num) {}
};

// Filter description
class FilterDescription {
   public:
    std::shared_ptr<Expr> condition;

    FilterDescription(std::shared_ptr<Expr> condition) : condition(condition) {}
};

#endif  // EXPR_TREE_H
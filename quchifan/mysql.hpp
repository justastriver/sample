//author : Andrew.PD
//date   : 2014/11/19 13:10:29
//ver    : 1.0
//desc   : mysql assistant

#ifndef _DB_MYSQL_H_
#define _DB_MYSQL_H_

#include <string>
#include <stdlib.h>
#include <string.h>
#include <vector>
#include <sys/timeb.h>
#include <iostream>
#include <tr1/unordered_map>

namespace common{
namespace db {
struct mysql_config_t {
    std::string mysql_host;
    std::string mysql_user;
    std::string mysql_pwd;
    std::string mysql_db;
    std::string mysql_table;
    int mysql_port;
    std::string mysql_select;
    std::string mysql_insert;
    std::string mysql_update;
};
class Mysql {
public:
    Mysql(void);
    ~Mysql(void);
public:
    static int Select(mysql_config_t &mysql_config,
                      std::string sql,
                      int expect_field_num /*IGNORE*/,
                      std::vector<std::vector<std::string> > &result,
                      std::string *error = NULL);
    static int Update(mysql_config_t &mysql_config, std::string sql,
                      std::string *error = NULL) {
        return Affect(mysql_config, sql, error);
    }
    static int Insert(mysql_config_t &mysql_config, std::string sql,
                      std::string *error = NULL) {
        return Affect(mysql_config, sql, error);
    }
  static int Insert(mysql_config_t &mysql_config, std::vector<std::string> &sqls,
                      std::string *error = NULL) {
        return Affect(mysql_config, sqls, error);
    }
    static int Delete(mysql_config_t &mysql_config, std::string sql,
                      std::string *error = NULL) {
        return Affect(mysql_config, sql, error);
    }
private:
    static int Affect(mysql_config_t &mysql_config, std::string sql,
                      std::string *error = NULL);
  static int Affect(mysql_config_t &mysql_config, std::vector<std::string> &sqls,
		    std::string *error = NULL);
};//class Mysql
}//namespace db
}//namespace common
#endif //_DB_MYSQL_H_

//author : Andrew.PD
//date   : 2014/11/19 13:10:29
//ver    : 1.0
//desc   : mysql assistant

#include <mysql/mysql.h>
#include <sys/timeb.h>
#include <stdio.h>
#include "mysql.hpp"
#include <iostream>
using namespace std;

namespace common {
namespace db {

Mysql::Mysql(void) {
}
Mysql::~Mysql(void) {
}
int Mysql::Select(mysql_config_t &mysql_config,
                  std::string sql,
                  int expect_field_num /*IGNORE*/,
                  std::vector<std::vector<std::string> > &result,
                  std::string *error) {

    MYSQL *mysql_data =  mysql_init(NULL);

    if (NULL == mysql_data) {
        return 1;
    }

    if (NULL == mysql_real_connect(mysql_data, mysql_config.mysql_host.c_str(),
                                   mysql_config.mysql_user.c_str(),
                                   mysql_config.mysql_pwd.c_str(),
                                   mysql_config.mysql_db.c_str(),
                                   mysql_config.mysql_port, NULL, 0)) {
        if (error) {
            *error = mysql_error(mysql_data);
        }

        return 2;
    }

    result.clear();
    mysql_query(mysql_data, "SET NAMES UTF8");

    if (0 != mysql_real_query(mysql_data, sql.c_str(), strlen(sql.c_str()))) {
        if (error) {
            *error = mysql_error(mysql_data);
        }

        mysql_close(mysql_data);
        return 3;
    }

    MYSQL_RES *res = mysql_store_result(mysql_data);
    MYSQL_ROW row;
    int num_fields =  mysql_num_fields(res);

    if (expect_field_num != num_fields ) {
        char tmp[100] = {0};
        snprintf(tmp, 100, "expect field(%d) is not equal to query field(%d)",
                 expect_field_num,
                 num_fields);
        *error = tmp;
        mysql_free_result(res);
        mysql_close(mysql_data);
        return 4;
    }

    while ((row = mysql_fetch_row(res)) != NULL) { //foreach order
        std::vector<std::string> vrow;

        for (int i = 0; i < num_fields; ++i) {
            if(row[i]) {
                vrow.push_back(row[i]);
            }
        }

        result.push_back(vrow);
    }

    if (result.size() == 0) {
        if (error) {
            *error = "query mysql return 0 row";
        }

        return 5;//that's ok
    }

    if (error) {
        *error = "successful";
    }

    mysql_free_result(res);
    mysql_close(mysql_data);
    return 0;
}
//private method
int Mysql::Affect(mysql_config_t &mysql_config, std::string sql, std::string *error) {

    MYSQL *mysql_data =  mysql_init(NULL);

    if (NULL == mysql_data) {
        return 1;
    }

    if (NULL == mysql_real_connect(mysql_data, mysql_config.mysql_host.c_str(),
                                   mysql_config.mysql_user.c_str(),
                                   mysql_config.mysql_pwd.c_str(),
                                   mysql_config.mysql_db.c_str(),
                                   mysql_config.mysql_port, NULL, 0)) {
        if (error) {
            *error = mysql_error(mysql_data);
        }

        return 1;
    }
    //set names
    mysql_query(mysql_data, "SET NAMES UTF8");
    
    if (0 != mysql_real_query(mysql_data, sql.c_str(), strlen(sql.c_str()))) {
        if (error) {
            *error = mysql_error(mysql_data);
        }

        mysql_close(mysql_data);
        return 1;
    }

    if (error) {
        *error = "successful";
    }

    mysql_close(mysql_data);
    return 0;
}
  //private method
  int Mysql::Affect(mysql_config_t &mysql_config, std::vector<std::string> &sqls,
		    std::string *error) {

    MYSQL *mysql_data =  mysql_init(NULL);

    if (NULL == mysql_data) {
        return 1;
    }

    if (NULL == mysql_real_connect(mysql_data, mysql_config.mysql_host.c_str(),
                                   mysql_config.mysql_user.c_str(),
                                   mysql_config.mysql_pwd.c_str(),
                                   mysql_config.mysql_db.c_str(),
                                   mysql_config.mysql_port, NULL, 0)) {
        if (error) {
            *error = mysql_error(mysql_data);
        }

        return 1;
    }
    //set names
    mysql_query(mysql_data, "SET NAMES UTF8");

    size_t size = sqls.size();
    for(size_t i = 0 ;i<size;++i) {

      if (0 != mysql_real_query(mysql_data, sqls[i].c_str(),
				strlen(sqls[i].c_str()))) {
        if (error) {
	  *error = mysql_error(mysql_data);
	  cout<<"[ERROR] sql ("<<i<<"): "<<sqls[i]<<endl;
        }

        mysql_close(mysql_data);
        return 1;
      }
    }

    if (error) {
        *error = "successful";
    }

    mysql_close(mysql_data);
    return 0;
}
}//namespace db
}//namespace common

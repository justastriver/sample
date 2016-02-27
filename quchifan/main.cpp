//g++ -g -o task main.cpp mysql.cpp -I./ -lmysqlclient -L/usr/lib64/mysql -s
#include <iostream>
#include <string>
#include <vector>
#include <map>

#include "mysql.hpp"

using namespace std;

int loadData(string datetime, vector<vector<string> > &data) {
  common::db::mysql_config_t cfg;
  cfg.mysql_host = "localhost";
  cfg.mysql_user = "root";
  cfg.mysql_pwd = "hello1234";
  cfg.mysql_db = "quchifan";
  cfg.mysql_table = "order";

  string sql = "select * from quchifan.order where date(created_at) = '"
    + datetime + "';";
  int expect_field_num = 43;

  string err = "";

  int res = common::db::Mysql::Select(cfg, sql, expect_field_num, data, &err);
  if(0 != res && 5 != res) {
    cout<<"[ERR] "<<err<<endl;
    return 1;
  }
  cout<<"[OK] load data ok, total records: "<<data.size()<<endl;
  
  return 0;
}
int merge() {
  /*
  vector<vector<string> >::iterator it_upper;
    
  for(it_upper = dbres.begin();it_upper != dbres.end();++it_upper) {
    vector<string>::iterator it_inner;
    for(it_inner = it_upper->begin();it_inner != it_upper->end();++it_inner) {
      cout<<"[Record] " << *it_inner<<endl;
    }
      
  }
  */
  return 0;

}
int updateData(vector<vector<string> > &data_new) {
  
  common::db::mysql_config_t cfg;
  cfg.mysql_host = "localhost";
  cfg.mysql_user = "root";
  cfg.mysql_pwd = "hello1234";
  cfg.mysql_db = "quchifan";
  cfg.mysql_table = "order";

  vector<vector<string> >::iterator it_upper;
  vector<string> sqls;    
  for(it_upper = data_new.begin();it_upper != data_new.end();++it_upper) {

    string value = "";
    vector<string>::iterator it_inner;
    int count = 0;

    for(it_inner = it_upper->begin();it_inner != it_upper->end();
	++it_inner,count++) {
      //cout<<"[Record] " << *it_inner<<endl;
      if(count == 0) {
	continue;//ignore id, increase auto
      }
      if(count == 42)
	value += "'" + *it_inner + "')";
      else 
	value += "'" + *it_inner + "',";
      
    }

    string sql = "insert into quchifan.order ("
      +string("order_no,  customer_id , restaurant_id , restaurant_name ," \
      "pay_type , prepay_id, init_price , customer_price , merchant_price,"\
      "knock_price , customer_pwd , wait_no , cabinet_no , cabinet ,"\
      "status , process_time,  payed , cancel_type , phone  , fetch_way ,"\
      "way,food_num,total_box_price , merchant_coupon_id , merchant_coupon_amount,"\
      "coupon_id ,coupon_amount, crop_coupon_id , crop_coupon_amount , remark,"\
      "people , arrived_at, source_from , platform , is_deleted , created_at,"\
      "updated_at , merchant_coupon , out_trade_no , out_trade_no_history ,"\
	      "origin_price , corp_discount) values(")
      + value;

    //cout<<"[DEBUG] sql" <<sql<<endl;
    sqls.push_back(sql);
    
    
  }
  string err = "";
  cout<<"[DEBUG] begin to update database"<<endl;
  int res = common::db::Mysql::Insert(cfg, sqls, &err);
  if(0 != res) {
    cout<<"[ERR] "<<err<<endl;
    return 1;
  }

  
  cout<<"[OK] update data ok, total records: "<<data_new.size()<<endl;
  
  return 0;
}
string parseTime(string base_datetime, string time) {
  string end = time.substr(time.find(' '));
  return base_datetime + end;
}
int doTask(vector<vector<string> > &data_base,
	   vector<vector<string> > &data_from,
	   string base_datetime) {
  //update data, filter userid and update the created_at, updated_at, arrived_at
  if(data_from.size() == 0) {
    cout<<"[WARN] from data is empty"<<endl;
    return 0;
  }

  map<unsigned long long, char> base_user_ids;
  
  vector<vector<string> > data_new;
  size_t data_base_size = data_base.size();
  size_t data_from_size = data_from.size();
  for(size_t i = 0;i<data_base_size;++i) {
    base_user_ids[atol(data_base[i][2].c_str())] = '1';
  }
  cout<<"[DEBUG] load to hash map, records: "<<base_user_ids.size()<<endl;

  vector<string> record;
  int count = 0;
  for(size_t i = 0;i<data_from_size;++i) {
    record = data_from[i];
    if(base_user_ids.end() != base_user_ids.find(atol(record[2].c_str()))) {
      //cout<<"[DEBUG] dumplicate record of user id : "<<record[2]<<endl;
      continue;
    }
    //record[0] = "111111111";//id
    //record[1] = "222222222223";//ordernum
    record[15] = "10";//status change to 10
    record[30] = "";//remark
    record[32] = parseTime(base_datetime,record[32]);//arrived_at
    record[36] = parseTime(base_datetime,record[36]);//created_at
    record[37] = parseTime(base_datetime,record[37]);//updated_at

    data_new.push_back(record);
    count++;
    if(count == 5000) {
      cout<<"[DEBUG] more than 5k records, ignore"<<endl;
      break;
    }
    //cout<<"[DEBUG] restaurant name : "<<record[4]<<endl;
  }
  if(updateData(data_new)) {
    cout<<"[ERROR] update data failed"<<endl;
    return 1;
  }
  
  return 0;
}
int main(int argc, char** argv) {
  if(argc != 3) {
    cout<< "[Usage] ./task base_datetime from_datetime"<<endl;
    cout<< "[Usage] ./task 2016-02-01 2016-01-01"<<endl;
    return 0;
  }
  string base_datetime = argv[1];
  string from_datetime = argv[2];

  //cout<<parseTime(base_datetime, "2016-01-01 12:00:00")<<endl;
  //return 0;
  
  vector<vector<string> > data_base;
  vector<vector<string> > data_from;

  cout<<"[DEBUG] loading base data"<<endl;
  if(loadData(base_datetime, data_base)) {
    cout<<"[ERROR] load base data failed"<<endl;
    return 0;
  }
  cout<<"[DEBUG] loading from data"<<endl;
  if(loadData(from_datetime, data_from)) {
    cout<<"[ERROR] load from data failed"<<endl;
    return 0;
  }
  if(doTask(data_base, data_from,base_datetime)) {
    cout<<"[ERROR] do task failed"<<endl;
    return 0;
  }


  return 0;
}

//g++ -g -o task-ex main.cpp mysql.cpp -I./ -lmysqlclient -L/usr/lib64/mysql -s
#include <iostream>
#include <string>
#include <vector>
#include <map>
#include <stdio.h>

#include "mysql.hpp"


const int STRATAGES[3][4] = {
  {25,20,15,12},
  {18,10,8,6},
  {15,8,6,4}
};

using namespace std;

typedef unsigned long long UUID;

struct RestItem{
  string resid;
  string name;
  string create_date;
};
struct OrderSizeItem{
  int origin_size;
  int curr_need_size;//will decrease
};

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



int loadRest(string datetime, map<UUID, RestItem> &restaurants) {
  
  common::db::mysql_config_t cfg;
  cfg.mysql_host = "localhost";
  cfg.mysql_user = "root";
  cfg.mysql_pwd = "hello1234";
  cfg.mysql_db = "quchifan";
  cfg.mysql_table = "order";
  //loadall data
  string sql = "select id, name,created_at from quchifan.restaurant where is_deleted = 0 and date(created_at) < '"
    + datetime + "' order by date(created_at) asc";
    
  int expect_field_num = 3;

  string err = "";
  vector<vector<string> > data;
  int res = common::db::Mysql::Select(cfg, sql, expect_field_num, data, &err);
  if(0 != res && 5 != res) {
    cout<<"[ERR] "<<err<<endl;
    return 1;
  }
  vector<vector<string> >::iterator it_upper;
    
  for(it_upper = data.begin();it_upper != data.end();++it_upper) {

      vector<string> &item = *it_upper;
      if(item.size() == 3) {
	UUID restid = atol(item[0].c_str());
	RestItem restitem;
	restitem.resid = item[0];
	restitem.name = item[1];
	restitem.create_date = item[2];
	restaurants[restid] = restitem;
	//cout<<"[DEBUG] add new restaurant :"<<restid<<", "<<restitem.name<<endl;
      }else {
	//err
	cout<<"[ERROR] field is not equel 3"<<endl;
      }
  }
  cout<<"[OK] load restaurant data ok, total records: "<<data.size()<<endl;
  
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
string parseTime(string base_datetime, string time, int needchange) {
  string end = time.substr(time.find(' '));
  //cout<<time<<endl;
  if(!needchange)  return base_datetime + end;

  string hourstring = end.substr(1,2);

  int hour = atoi(hourstring.c_str());
  if(hour < 16)
    hour += 6;
  //cout<<"[DEBUG] origin time:"<<base_datetime<<", hour to "<<hour<<endl;
  char buff[20] = {0};
  snprintf(buff,sizeof(buff)," %d%s",hour,end.substr(3).c_str());
  return base_datetime + buff;
}
int doTask(vector<vector<string> > &data_base,
	   vector<vector<string> > &data_from,
	   map<UUID, RestItem> &restaurants,
	   string base_datetime,
	   int count_of_restaurant,int type, int way
	   ) {
  
  //update data, filter userid and update the created_at, updated_at, arrived_at
  if(data_from.size() == 0) {
    cout<<"[WARN] from data is empty"<<endl;
    return 0;
  }
  
  map<unsigned long long, char> base_user_ids;
  
  vector<vector<string> > data_new;
  size_t data_base_size = data_base.size();
  size_t data_from_size = data_from.size();

  map<UUID, RestItem>::iterator iter_restaurants;
  map<UUID, OrderSizeItem/*order size*/> restaurants_with_order;
  map<UUID, OrderSizeItem/*order size*/>::iterator iter_restaurants_with_order;
  UUID restid;
  int total_order_size = 0;
  for(size_t i = 0;i<data_base_size;++i) {
    base_user_ids[atol(data_base[i][2].c_str())] = '1';
    //here, 2016-03-07
    restid = atol(data_base[i][3].c_str());
    total_order_size += atoi(data_base[i][22].c_str());
    //get sum of orders
    if(restaurants_with_order.find(restid)
       == restaurants_with_order.end()) {
      OrderSizeItem item;
      item.origin_size = 1;
      item.curr_need_size = 0;
      restaurants_with_order[restid] = item;
    }else {
      restaurants_with_order[restid].origin_size++;
    }
  }
  cout<<"[DEBUG] rest with order size is ("<<restaurants_with_order.size()
      <<","<<total_order_size<<")"<<endl;
  cout<<"[DEBUG] load to hash map, records: "<<base_user_ids.size()<<endl;

  //set stratages
  int indexer = 0;
  int count = data_from.size();
  vector<string> record;
  int willchange_time = 0;
  for(iter_restaurants_with_order = restaurants_with_order.begin();
      iter_restaurants_with_order != restaurants_with_order.end();
      ++iter_restaurants_with_order) {
    //more than 150
    OrderSizeItem &item = iter_restaurants_with_order->second;
    if(item.origin_size > 150) {
      item.curr_need_size = STRATAGES[way][0];
    }else if(item.origin_size <= 150 && item.origin_size >80) {
      item.curr_need_size = STRATAGES[way][1];
    }else if(item.origin_size <= 80 && item.origin_size >30) {
      item.curr_need_size = STRATAGES[way][2];
    }else if(item.origin_size <= 30 && item.origin_size >0) {
      item.curr_need_size = STRATAGES[way][3];
    }else {//not exist
    }

    iter_restaurants = restaurants.find(iter_restaurants_with_order->first);
    if(iter_restaurants == restaurants.end()) {
      cout<<"[ERROR] not found restaurant id: "<<iter_restaurants_with_order->first<<endl;
      continue;
    }
    RestItem &rest = iter_restaurants->second;
    /*
    cout<<"[DEBUG] need add normal size is :"<<item.curr_need_size
	<<", from size:"<<item.origin_size
	<<", for restid:"<<rest.name<<endl;
    */
    if(indexer > count) {
      cout<<"[WARN] index is over flow..."<<count<<endl;
      break;
    }
    for(int i = 0;i<item.curr_need_size;++i) {
      record = data_from[indexer % count];
      indexer++;
      if(indexer > count) {
	cout<<"[WARN] index is over flow..."<<count<<endl;
	break;
      }
      /*
	if(base_user_ids.end() != base_user_ids.find(atol(record[2].c_str()))) {
	//cout<<"[DEBUG] dumplicate record of user id : "<<record[2]<<endl;
	continue;
	}
      */
      //add new
      //record[0] = "111111111";//id
      //record[1] = "222222222223";//ordernum


      record[3] = rest.resid;//restuarant_id
      record[4] = rest.name;//restuarant_name
      record[15] = "10";//status change to 10
      record[30] = "";//remark
      willchange_time = rand() % 2;
      record[32] = parseTime(base_datetime,record[32],willchange_time);//arrived_at
      record[36] = parseTime(base_datetime,record[36],willchange_time);//created_at
      record[37] = parseTime(base_datetime,record[37],willchange_time);//updated_at
      total_order_size += atoi(record[22].c_str());

      data_new.push_back(record);

    }

  }
  int total_rest_with_order = restaurants_with_order.size();
  int total_add_zero_rest = 0;
  //set 0 orders restaurants
  for(iter_restaurants =restaurants.begin();
      iter_restaurants != restaurants.end();
      iter_restaurants++) {
    if(indexer > count) {
      cout<<"[WARN] index is over flow..."<<count<<endl;
      break;
    }
    iter_restaurants_with_order = restaurants_with_order.find(iter_restaurants->first);
    if(iter_restaurants_with_order == restaurants_with_order.end()) {
      //is null, add order
      int need_size = rand() % STRATAGES[way][3] + 1;
      if(need_size !=0) {
	total_rest_with_order++;
	total_add_zero_rest++;
	count_of_restaurant--;
	if(count_of_restaurant<1) {
	  break;
	}
      }
      RestItem &rest = iter_restaurants->second;
      /*
      cout<<"[DEBUG] need add random size is :"<<need_size
	  <<", from size: 0"
	  <<", for restid:"<<rest.name<<endl;
      */
      for(int i = 0;i<need_size;++i) {
	record = data_from[indexer % count];
	indexer++;
	if(indexer > count) {
	  cout<<"[WARN] index is over flow..."<<count<<endl;
	  break;
	}
	//add new
	//record[0] = "111111111";//id
	//record[1] = "222222222223";//ordernum
	
	record[3] = rest.resid;//restuarant_id
	record[4] = rest.name;//restuarant_name
	record[15] = "10";//status change to 10
	record[30] = "";//remark
	willchange_time = rand() % 2;
	record[32] = parseTime(base_datetime,record[32],willchange_time);//arrived_at
	record[36] = parseTime(base_datetime,record[36],willchange_time);//created_at
	record[37] = parseTime(base_datetime,record[37],willchange_time);//updated_at
	total_order_size += atoi(record[22].c_str());
	
	data_new.push_back(record);
      }
    }
    
	
  }
  cout<<"[DEBUG] total rest with order is ("<<total_rest_with_order
      <<","<<total_order_size<<")"
      <<",zero add: "<<total_add_zero_rest
      <<endl;

  cout<<"[DEBUG] will update data size: "<<data_new.size()<<endl;

  if(type) {
    if(updateData(data_new)) {
      cout<<"[ERROR] update data failed"<<endl;
      return 1;
    }
  }
  cout<<"[OK] total size : ("<<data_base.size() + data_new.size()
      <<","<<total_order_size<<")"
      <<endl;
  return 0;
}
int main(int argc, char** argv) {
  //cout<<parseTime("2016-03-08", "2016-05-01 10:13:33", 1)<<endl;
  //cout<<parseTime("2016-03-08", "2016-05-01 13:13:33", 1)<<endl;
  //cout<<parseTime("2016-03-08", "2016-05-01 17:13:33", 1)<<endl;
  
  if(argc != 6) {
    cout<< "[Usage] ./task base_datetime from_datetime count_of_restaurant type(0-test,1-realjob) stratege(0,1)"<<endl;
    cout<< "[Usage] ./task 2016-02-01 2016-01-01 10 0 0"<<endl;
    return 0;
  }
  string base_datetime = argv[1];
  string from_datetime = argv[2];
  int count_of_restaurant = atoi(argv[3]);
  int type = atoi(argv[4]);
  int way = atoi(argv[5]);

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
  map<UUID, RestItem> restaurants;
  if(loadRest(base_datetime,restaurants)) {
    cout<<"[ERROR] load restaurant from data failed"<<endl;
    return 0;
  }
  if(doTask(data_base, data_from, restaurants,
	    base_datetime,count_of_restaurant , type, way)) {
    cout<<"[ERROR] do task failed"<<endl;
    return 0;
  }
  

  return 0;
}

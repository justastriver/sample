#step 1, load current order
mysql -uroot -phello1234 -Dquchifan <~/download/sqlresult_418317.sql

#step 2, run task to parse and update database
./task 2016-02-26 2016-02-01

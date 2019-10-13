-- 查看以“sp_”开头的存储过程状态
show procedure status like 'sp_%';
-- 查看指定存储过程的创建语句
show create procedure sp_batchinsert;

-- 查看以“sf_”开头的函数状态
show function status like 'sf_%';
-- 查看指定函数的创建语句
show create function sf_add;

-- 把结束符定义为“//”
delimiter //
drop procedure if exists sp_batchinsert//
create procedure sp_batchinsert(in n int)
begin
	declare name0 varchar(20);
	declare age0 int;
	declare i int default 0;
	while i < n do
		set name0 := concat('apollo', i);
		set age0 := i % 100;
		insert into users(name, age) values(name0, age0);
		set i := i + 1;
	end while;
end
//

delimiter //
drop function if exists sf_add//
create function sf_add(a int, b int) returns int
begin
	return a+b;
end
//

create database hive;
use hive;

select * from dbs;

select * from tbls;
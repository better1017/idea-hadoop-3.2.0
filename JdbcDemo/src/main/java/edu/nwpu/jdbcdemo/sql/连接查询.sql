-- 如果存在先删除表
drop table if exists customers;
drop table if exists orders;

-- 建表
create  table customers(
    id int primary key auto_increment,
    name varchar(20),
    age int
); -- 创建customers表
create table orders(
    id int primary key auto_increment,
    orderno varchar(20),
    price float,
    cid int
); -- 创建orders表

-- 插入顾客数据
insert into customers(name,age) values('tom',12);
insert into customers(name,age) values('tomas',13);
insert into customers(name,age) values('tomasLee',14);
insert into customers(name,age) values('tomason',15);

select * from customers;

-- 插入订单数据
insert into orders(orderno,price,cid) values('No001',12.25,1);
insert into orders(orderno,price,cid) values('No002',12.30,1);
insert into orders(orderno,price,cid) values('No003',12.25,2);
insert into orders(orderno,price,cid) values('No004',12.25,2);
insert into orders(orderno,price,cid) values('No005',12.25,2);
insert into orders(orderno,price,cid) values('No006',12.25,3);
insert into orders(orderno,price,cid) values('No007',12.25,3);
insert into orders(orderno,price,cid) values('No008',12.25,3);
insert into orders(orderno,price,cid) values('No009',12.25,3);
insert into orders(orderno,price,cid) values('No0010',12.25,NULL);

select * from orders;

-- 连接查询
-- 笛卡尔积查询,无连接条件查询
select a.*,b.* from customers a , orders b;
-- 内连接,查询符合条件的记录.
select a.*,b.* from customers a , orders b where a.id = b.cid;
-- 左外连接,查询符合条件的记录.
select a.*,b.* from customers a left outer join orders b on a.id = b.cid;
-- 右外连接，查询符合条件的记录
select a.*, b.* from customers a right outer join orders b on a.id = b.cid;
-- mysql不支持全外连接






















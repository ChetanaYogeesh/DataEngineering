drop table if exists stripe.event_log	
;	
	
create table stripe.event_log (	
	id varchar(50) 
	,event_type varchar(50)
	,object_event_id varchar(50)
	,object varchar(20)
	,customer_id varchar(50)
	,email char(32)
	,amount int
	,amount_due int
	,amount_refunded int
	,event_datetime datetime
	,event_unixtimestamp bigint primary key
	,unique index(event_id)
	,index(email)
	,index(event_type)
	,index(event_datetime)
)	
;	

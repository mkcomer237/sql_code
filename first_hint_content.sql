




--1. get new trialers who registered at the same time ------------------------------------------------------------------------------------------------------------------------------------


select top 100 * from p.fact_subscription fs
go
select top 100 * from p.fact_registrant fr


select
    fr.ucdmid,
    signupcreatedate,
    fr.ancestryregistrationdate,
    netbillthroughquantity
into a.max_new_signups
from p.fact_registrant fr
inner join p.dim_registrationtype ty on ty.registrationtypeid = fr.registrationtypeid
inner join p.fact_subscription fs on fs.prospectid = fr.prospectid
left join s.webservice w on (fs.billthroughwebserviceid=w.webserviceid)
left join p.dim_programout b on (fs.grossprogramoutid=b.programoutid)
left join p.dim_duration d on (fs.billthroughdurationid=d.durationid) --change to signup for billthrough  
left join p.dim_subscription s on (fs.billthroughsubscriptionid=s.subscriptionid)
left join p.dim_trialtype tt on tt.trialtypeid = fs.trialtypeid
left join p.dim_programin program on fs.programinid = program.programinid
where 
    fs.trialtypeid = 1 --this is how you identify the free trial row
    and signupcreatedate between '2015-01-01' and '2015-05-01'
    and program.programinparentdescription = 'New' --not necessary (or include cross sell)?\
    and trunc(ancestryregistrationdate) = trunc(signupcreatedate) 
order by 1, 2



--keep only people signing up and registering within an hour 

select count(*), count(distinct ucdmid) from a.max_new_signups
delete from a.max_new_signups where datediff(minute, ancestryregistrationdate, signupcreatedate) > 30


--2. Add in the first tree and keeep only people who created a tree early on ------------------------------------------------------------------------------------------------------------------------------------


select 
    ns.*,
    ft.treeid,
    Case When ft.treecreatedate between '3/8/2015 02:00:00 AM' and '11/1/2015 02:00:00 AM' then DateAdd (hour,1,ft.treecreatedate) Else ft.treecreatedate End,
    row_number() over (partition by ns.ucdmid order by treecreatedate) as tree_number
into a.max_new_signups_tree
from a.max_new_signups ns
inner join p.fact_trees ft on ft.ucdmid = ns.ucdmid

delete from a.max_new_signups_tree where tree_number <> 1 
delete from a.max_new_signups_tree where datediff(minute, ancestryregistrationdate, treecreatedate) > 60

select count(*), count(distinct ucdmid) from a.max_new_signups_tree



--3. add in hints and personacreatedate for this the user/tree/signup/reg (14 mins) ------------------------------------------------------------------------------------------------------------------------------------


drop table a.max_new_signups_hint
go
select 
    ns.*,
    rh.hintid,
    rh.personaid,
    rh.dbid,
    rh.score,
    Case When rh.datecreated between '3/8/2015 02:00:00 AM' and '11/1/2015 02:00:00 AM' then DateAdd (hour,-6,rh.datecreated) Else dateadd(hour, -7, rh.datecreated) End as hintcreatedate,
    rh.status,
    Case When p.datestampcreated between '3/8/2015 02:00:00 AM' and '11/1/2015 02:00:00 AM' then DateAdd (hour,1,p.datestampcreated) Else p.datestampcreated End as personacreatedate,
    row_number() over (partition by ns.ucdmid order by rh.datecreated) as hint_number
into a.max_new_signups_hint
from a.max_new_signups_tree ns
inner join s.recordhints rh on rh.treeid = ns.treeid
inner join s.personas p on p.personaid = rh.personaid
order by
    ns.ucdmid,
    rh.datecreated


--3.5 [skip] how long after tree creation are the hints added? ------------------------------------------------------------------------------------------------------------------------------------



select 
    datediff(hour, treecreatedate, hintcreatedate),
    count(*)
from a.max_new_signups_hint
where hintcreatedate is not null
group by 
    datediff(hour, treecreatedate, hintcreatedate)
order by 1


--see if this is on certain days only? 


select 
    trunc(treecreatedate) as treecreatedate,
    count(*)
from a.max_new_signups_hint
where 
    hintcreatedate is not null
    and datediff(hour, treecreatedate, hintcreatedate) < 0
    and treecreatedate >= '1/1/2015'
group by 
    trunc(treecreatedate)
order by 1


--hint is created before the tree existed or user registered?  


select * from s.recordhints where hintid = 93703876815
go
select * from p.fact_trees where treeid = 79164598
go
select * from s.personas where personaid = 46399630069


--4. Find out what DBID of hints are created right after registration  ------------------------------------------------------------------------------------------------------------------------------------


select 
    hint_number,
    dbid,
    databasetitle,
    databasecategorydesc,
    count(*)
from a.max_new_signups_hint ns
left join p.dim_database dd on dd.databaseid = ns.dbid
where 
    datediff(hour, ancestryregistrationdate, hintcreatedate) between 0 and 2
    and hint_number <= 10
group by 
    hint_number,
    dbid,
    databasetitle,
    databasecategorydesc
order by 3 desc  


-- Add in hint create minute and keep only the first 120 minutes (tableau dashboard runs off of this) 


drop table a.max_new_signups_hint_minute
go
select
    h.ucdmid,
    hintid,
    score,
    status,
    hint_number,
    datediff(minute, treecreatedate, hintcreatedate) as create_minute,
    h.dbid,
    databasetitle,
    databasecategorydesc,
    case when imagesserviceaccess is null or imagesserviceaccess = 0 then 'No Image' else 'Has Image' end as has_image
into a.max_new_signups_hint_minute
from a.max_new_signups_hint h
left join p.dim_database dd on dd.databaseid = h.dbid 
where
    datediff(minute, treecreatedate, hintcreatedate) <= 120
    and signupcreatedate >= '2015-02-01' 


GRANT SELECT ON a.max_new_signups_hint_minute TO tableau








-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--Granular Version

--1.  Trees, CV and Searches (~2 mins)


drop table a.max_uss_engagment
go

select
    a.*,
    b.visits,
    b.visit_minutes,
    b.pageviews,
    case when c.signup_ucdmid is null then 'No' else 'Yes' end as started_trial
into a.max_uss_engagment
from ( 
    select
        sortid as sort,
        case    when lower(cellid) like '%control%' then 'Control - ' || cellnumber
                else 'Test - ' || cellnumber end as cellid,
        ds.locality,
        dr.regtype,
        ut.usertypedescription,
        trunc(actiondate) as assignmentdate,
        ta.ucdmid,
        sum(isnull(fe.newsearches,0)) searches,
        sum(isnull(fe.contentviews,0)) contentviews,
        sum(isnull(fe.nodescreated,0)) nodescreated,
        sum(isnull(fe.treescreated,0)) treescreated,
        sum(isnull(fe.treehintsaccepted,0)) as "Hints Tree Accepted",
        sum(isnull(fe.recordhintsaccepted,0)) as "Hints Record Accepted",
        sum(isnull(fe.objecthintsaccepted,0)) as "Hints Object Accepted",
        sum(isnull(fe.treehintsadded,0)) as "Hints Tree Added",
        sum(isnull(fe.recordhintsadded,0)) as "Hints Record Added",
        sum(isnull(fe.objecthintsadded,0)) as "Hints Object Added",
        sum(isnull(fe.treehintsrejected,0)) as "Hints Tree rejected",
        sum(isnull(fe.recordhintsrejected,0)) as "Hints Record rejected",
        sum(isnull(fe.objecthintsrejected,0)) as "Hints Object rejected",

        count(distinct case when fe.newsearches + fe.contentviews + fe.successnew + fe.discoveries + fe.photosuploaded + fe.storiescreated + fe.treehintsaccepted + fe.recordhintsaccepted +fe.objecthintsaccepted +
        fe.treehintsrejected + fe.recordhintsrejected +fe.objecthintsrejected + fe.treescreated + fe.nodescreated + fe.treeinvitationsent + fe.treeinvitationsent + fe.objectcommentsfromtree + 
        fe.likesfromtree + fe.rootmessages >= 1 then fe.dayid else null end) as activedays

    from (
        select 
            cast(usc.sortid as varchar(10)) || ' - ' || sortname as sortid,
            cast(usc.cellid as varchar(6)) || '-' || cellname || '-' || cellvalue as cellid,
            usc.cellid as cellnumber,
            upper(ucdmid) as ucdmid,
            Case When actiondate between '3/8/2015 02:00:00 AM' and '11/1/2015 02:00:00 AM' then DateAdd(hour,-6,actiondate) Else DateAdd(hour,-7,actiondate) End as actiondate
        from p.fact_usersortcustomer as usc
        left join (select distinct sortid, sortname from s.usersortsort) as uss on uss.sortid = usc.sortid
        left join (Select cellid, max(cellname) as cellname, max(cellvalue) as cellvalue from s.usersortcell group by cellid) as cell on cell.cellid = usc.cellid
        where 
            Case When actiondate between '3/8/2015 02:00:00 AM' and '11/1/2015 02:00:00 AM' then DateAdd(hour,-6,actiondate) Else DateAdd(hour,-7,actiondate) End <= trunc(getdate())
            and usc.cellid in (8408, 7270, 8431, 8433, 8435, 8437, 8439, 7388, 7390)
            and trunc(Case When actiondate between '3/8/2015 02:00:00 AM' and '11/1/2015 02:00:00 AM' then DateAdd(hour,-6,actiondate) Else DateAdd(hour,-7,actiondate) End) <=  (select max(dayid) from p.fact_engagement)
            ) as ta
    inner join p.dim_date dd on trunc(dd.datevalue) = trunc(ta.actiondate)
    inner join p.fact_registrant fr on fr.ucdmid = ta.ucdmid
    left join p.dim_registrationtype dr on dr.registrationtypeid = fr.registrationtypeid
    left join p.dim_site ds on ds.siteid = fr.registrationsiteid
    left join p.fact_engagement fe on 
        ta.ucdmid = fe.ucdmid
        and fe.dayid >= trunc(actiondate)
        and fe.dayid < dateadd(day, (select * from a.max_uss_eng_window), trunc(actiondate)) -- engagement within 15 days of the test assignemnt 
    left join s.userstatus us on 
        us.ucdmid = ta.ucdmid
        and dateadd(minute, 1, ta.actiondate) >= us.indate 
        and dateadd(minute, 1, ta.actiondate) < us.outdate
    left join p.dim_usertype ut on ut.usertypeid = us.usertypeid
    group by
        sortid,
        case    when lower(cellid) like '%control%' then 'Control - ' || cellnumber
                else 'Test - ' || cellnumber end, 
        ds.locality,
        ds.locality,
        dr.regtype,
        ut.usertypedescription,
        trunc(actiondate),
        ta.ucdmid) as a
left join (
    select
        ta.ucdmid,
        sum(coalesce(visits, 0)) as visits,
        sum(coalesce(visitdurationminutes, 0)) as visit_minutes,
        sum(coalesce(pageviews, 0)) as pageviews
    from (
        select 
            usc.cellid,
            upper(ucdmid) as ucdmid,
            usc.sortid,
            sortname,
            Case When actiondate between '3/8/2015 02:00:00 AM' and '11/1/2015 02:00:00 AM' then DateAdd(hour,-6,actiondate) Else DateAdd(hour,-7,actiondate) End as actiondate
        from p.fact_usersortcustomer as usc
        left join (select distinct sortid, sortname from s.usersortsort) as uss on uss.sortid = usc.sortid
        left join (Select cellid, max(cellname) as cellname, max(cellvalue) as cellvalue from s.usersortcell group by cellid) as cell on cell.cellid = usc.cellid
        where 
            Case When actiondate between '3/8/2015 02:00:00 AM' and '11/1/2015 02:00:00 AM' then DateAdd(hour,-6,actiondate) Else DateAdd(hour,-7,actiondate) End <= trunc(getdate())
            and usc.cellid in (8408, 7270, 8431, 8433, 8435, 8437, 8439, 7388, 7390)
            and trunc(Case When actiondate between '3/8/2015 02:00:00 AM' and '11/1/2015 02:00:00 AM' then DateAdd(hour,-6,actiondate) Else DateAdd(hour,-7,actiondate) End) <=  (select max(dayid) from p.fact_engagement)
            ) as ta
    left join p.fact_visits fv on 
        fv.ucdmid = ta.ucdmid
        and servertimemst > actiondate
        and trunc(servertimemst) <= dateadd(day, (select * from a.max_uss_eng_window), trunc(actiondate))
    group by
        ta.ucdmid) as b
on a.ucdmid = b.ucdmid
left join ( --find out if they started a new trial after being put in the test 
    select
        ta.ucdmid,
        max(fs.ucdmid) as signup_ucdmid
    from (
        select 
            usc.cellid,
            upper(ucdmid) as ucdmid,
            usc.sortid,
            sortname,
            Case When actiondate between '3/8/2015 02:00:00 AM' and '11/1/2015 02:00:00 AM' then DateAdd(hour,-6,actiondate) Else DateAdd(hour,-7,actiondate) End as actiondate
        from p.fact_usersortcustomer as usc
        left join (select distinct sortid, sortname from s.usersortsort) as uss on uss.sortid = usc.sortid
        left join (Select cellid, max(cellname) as cellname, max(cellvalue) as cellvalue from s.usersortcell group by cellid) as cell on cell.cellid = usc.cellid
        where 
            Case When actiondate between '3/8/2015 02:00:00 AM' and '11/1/2015 02:00:00 AM' then DateAdd(hour,-6,actiondate) Else DateAdd(hour,-7,actiondate) End <= trunc(getdate())
            and usc.cellid in (8408, 7270, 8431, 8433, 8435, 8437, 8439, 7388, 7390)
            and trunc(Case When actiondate between '3/8/2015 02:00:00 AM' and '11/1/2015 02:00:00 AM' then DateAdd(hour,-6,actiondate) Else DateAdd(hour,-7,actiondate) End) <=  (select max(dayid) from p.fact_engagement)
            ) as ta
    left join p.fact_subscription fs on 
        ta.ucdmid = fs.ucdmid 
        and fs.trialtypeid = 1
        and fs.signupcreatedate >= ta.actiondate
    group by
        ta.ucdmid) as c
on c.ucdmid = a.ucdmid

go


GRANT SELECT ON a.max_uss_engagment TO tableau


--new table with only cellids 


cellid in (8236, 8238, 8408, 7274)


select top 1000 * from a.max_uss_engagment order by dayid desc


--select count(*) from a.max_uss_engagment


select top 100 * from p.fact_usersortcustomer order by actiondate desc


select trunc(signupcreatedate) <=  (select max(dayid) from p.fact_engagement)


drop table a.max_uss_sorts 
go
create table a.max_uss_sorts (sortid int)


go
insert into a.max_uss_sorts values (1712)
go
insert into a.max_uss_sorts values (1720)
go
insert into a.max_uss_sorts values (1722)



drop table a.max_uss_bad_cells 
go
create table a.max_uss_bad_cells (cellid int)

go
insert into a.max_uss_bad_cells values (8192)
go
insert into a.max_uss_bad_cells values (8194)
go
insert into a.max_uss_bad_cells values (8204)
go
insert into a.max_uss_bad_cells values (8206)
go
insert into a.max_uss_bad_cells values (8232)
go
insert into a.max_uss_bad_cells values (8240)
go
insert into a.max_uss_bad_cells values (8242)
go
insert into a.max_uss_bad_cells values (8244)
go
insert into a.max_uss_bad_cells values (8246)
go
insert into a.max_uss_bad_cells values (8254)
go
insert into a.max_uss_bad_cells values (8252)
go
insert into a.max_uss_bad_cells values (8234)


--select top 100 * from p.fact_engagement order by dayid desc


drop table a.max_uss_eng_window
go
select 14 into a.max_uss_eng_window


select * from a.max_uss_eng_window


GRANT SELECT ON a.max_uss_eng_window TO tableau
go
GRANT SELECT ON a.max_uss_bad_cells TO tableau
go
GRANT SELECT ON a.max_uss_sorts TO tableau


/*


select top 1000 * from  a.max_uss_engagment where activedays >= 1


--2.  Check some specific cases against the tablea report ----------------------------------------------------------------------------------------

select top 10000 * from a.max_uss_engagment 

--added per adding 

select
    cellid,
    count(distinct ucdmid),
    count(distinct case when "hints tree added" >= 1 and "hints tree added" <= 50000 then ucdmid else null end) as tree_hint_users,
    sum(case when "hints tree added" <= 50000 then "hints tree added" else null end) as tree_hints_added,
    variance(case when "hints tree added" >= 1 and "hints tree added" <= 50000 then "hints tree added" else null end) as tree_hints_added_var,
    count(distinct case when "hints tree accepted" >= 1 and "hints tree accepted" <= 10000 then ucdmid else null end) as tree_hint_accepted_users,
    sum(case when "hints tree accepted" <= 10000 then "hints tree accepted" else null end) as tree_hints_accepted,
    variance(case when "hints tree accepted" >= 1 and "hints tree accepted" <= 10000 then "hints tree accepted" else null end) as tree_hints_accepted_var
from a.max_uss_engagment 
where sort = '1712 - Header.Version2'
group by
    cellid
order by 1



select
    cellid,
    count(distinct ucdmid),
    count(distinct case when "hints tree added" >= 1 then ucdmid else null end) as tree_hint_users,
    count(distinct case when "hints tree accepted" >= 1 then ucdmid else null end) as tree_hint_accepted_users,
    sum("hints tree added") as "hints tree added",
    sum("hints tree accepted") as "hints tree accepted",
    sum("hints tree rejected") as "hints tree rejected"
from a.max_uss_engagment 
where sort = '1712 - Header.Version2'
group by
    cellid
order by 1



select
    cellid,
    sum("hints tree added")
from a.max_uss_engagment 
group by
    cellid



--3.  Active Users 




*/







--Total Run Time ~15 mins

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--I.  Select the cohorts to use (with enough data), and calculate the survival curves for these --------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------



--declare parameters 


use Typhoon
go
set ansi_warnings off
go
set nocount on
go


create procedure dbo.calculate_ltr
	@fees float,
	@rolling_months int,
	@discount_rate float,
	@data_cutoff int,
	@mult_customer_cutoff int,
	@pct_cutoff float
as

-- create a table to store renewal rate results  



if  object_id('tempdb..#renewal_rates') is not null 
drop table tempdb..#renewal_rates ; 
create table #renewal_rates (product_category_fine varchar(128), aggregation_level varchar (128),  aggregation_value varchar(255),  order_month int, net_sale_count smallint, cumulative_sales int, grouped_cumulative_sales int, month_number int, duration smallint, renewal_rate float, rolling_renewal_rate float, max_sale_count smallint)



-- create a temp table of the data being used to be accessed each loop and improve performance (customer leve)


if  object_id('tempdb..#msid') is not null 
drop table tempdb..#msid ; 
select distinct 
	orderitem,
	msid,
	customercampaign
into #msid
from typhoon.dbo.typhoon_transaction_items tti with(nolock)



if  object_id('tempdb..#fact') is not null 
drop table tempdb..#fact ; 
select 
	td.[month],
	toir.[Net Sale Count],
	toir.[Is Trial Product],
	toir.SubscriptionDurationMonths,
	toir.[Subscription Price],
	case when toir.[Is Trial Product] = 1 then 'Trial' else 'HO' end + ' ' + cast(toir.SubscriptionDurationMonths as varchar(2)) + ' Month ' + cast(toir.[Subscription Price] as varchar(10)) as product_category_fine,
	tc.ChannelName,
	tc.CampaignGroupName,
	tc.OriginName + 
		case when ChannelName = 'SEM' and Originname not like '%Mobile%' then  --something wrong here also 
			case when CHARINDEX('_d', msid, 1) = 0 then ' Desktop' else 
				case	when SUBSTRING(msid,CHARINDEX('_d', msid, 1) + 2, 1) = 'c' then ' Desktop' 
						when SUBSTRING(msid,CHARINDEX('_d', msid, 1) + 2, 1) = 't' then ' Desktop' 
						when SUBSTRING(msid,CHARINDEX('_d', msid, 1) + 2, 1) = 'm' then ' Mobile' 
						when MSID is null then ' Desktop'  
						else ' error' end 
				end
		else '' end as OriginName,
	tc.Campaign,
	1 as test
into #fact
from Typhoon.dbo.Typhoon_Order_Items_RollUp as toir WITH(NOLOCK)
	inner join #msid tti on tti.orderitem = toir.orderitem
	inner join Typhoon.dbo.Typhoon_Date as td WITH(NOLOCK) on td.DateKey = toir.[OrderDate]
	inner join Typhoon.dbo.Typhoon_Customer_Rollup as tcr WITH(NOLOCK) on tcr.Customer = toir.Customer
	inner join Typhoon.dbo.Typhoon_Campaigns as tc WITH(NOLOCK) on tc.Campaign = tcr.Campaign
where 
	toir.[Is Subscription Product] = 1 
	and toir.Domain = 'Archives.com'
	and toir.[Is Trial Product] = 1 --allow the upgrade from free account non primary product in
	and (toir.businesspartner = 0 or toir.businesspartner = 11)
order by
	td.[month],
	toir.[Net Sale Count]




--create a table with only campaign/channel/origins with enough data to improve performance, and also remove quotes from the names


if  object_id('tempdb..#good_campaigns') is not null 
drop table tempdb..#good_campaigns ; 
select
	tc.ChannelName,
	tc.OriginName + 
		case when ChannelName = 'SEM' and Originname not like '%Mobile%' then  --something wrong here also 
			case when CHARINDEX('_d', msid, 1) = 0 then ' Desktop' else 
				case	when SUBSTRING(msid,CHARINDEX('_d', msid, 1) + 2, 1) = 'c' then ' Desktop' 
						when SUBSTRING(msid,CHARINDEX('_d', msid, 1) + 2, 1) = 't' then ' Desktop' 
						when SUBSTRING(msid,CHARINDEX('_d', msid, 1) + 2, 1) = 'm' then ' Mobile' 
						when MSID is null then ' Desktop'  
						else ' error' end 
				end
		else '' end as OriginName,
	tc.CampaignGroupName,
	tc.Campaign,
	count(distinct tti.orderitem) as orders
into #good_campaigns
from Typhoon.dbo.Typhoon_Order_Items_RollUp as toir WITH(NOLOCK)
	inner join #msid tti on tti.orderitem = toir.orderitem
	inner join Typhoon.dbo.Typhoon_Customer_Rollup as tcr WITH(NOLOCK) on tcr.Customer = toir.Customer
	inner join Typhoon.dbo.Typhoon_Campaigns as tc WITH(NOLOCK) on tc.Campaign = tcr.Campaign
where 
	toir.[Is Subscription Product] = 1 
	and toir.Domain = 'Archives.com'
	and toir.[Is Trial Product] = 1 --allow the upgrade from free account non primary product in
	and (toir.businesspartner = 0 or toir.businesspartner = 11)
group by
	tc.ChannelName,
	tc.OriginName + 
		case when ChannelName = 'SEM' and Originname not like '%Mobile%' then
			case when CHARINDEX('_d', msid, 1) = 0 then ' Desktop' else 
				case	when SUBSTRING(msid,CHARINDEX('_d', msid, 1) + 2, 1) = 'c' then ' Desktop' 
						when SUBSTRING(msid,CHARINDEX('_d', msid, 1) + 2, 1) = 't' then ' Desktop' 
						when SUBSTRING(msid,CHARINDEX('_d', msid, 1) + 2, 1) = 'm' then ' Mobile' 
						when MSID is null then ' Desktop'  
						else ' error' end 
				end
		else '' end,
	tc.CampaignGroupName,
	tc.Campaign
having count(*) >= 500 

drop table #msid


--get only products with a certain amount of total orders for performance



if  object_id('tempdb..#good_products') is not null 
drop table tempdb..#good_products ; 
select 
	toir.[Is Trial Product],
	toir.SubscriptionDurationMonths,
	toir.[Subscription Price],
	case when [Is Trial Product] = 1 then 'Trial' else 'HO' end + ' ' + cast(toir.SubscriptionDurationMonths as varchar(2)) + ' Month ' + cast(toir.[Subscription Price] as varchar(10)) as product_category_fine,
	case when [Is Trial Product] = 1 then 'Trial' else 'HO' end + ' ' + cast(toir.SubscriptionDurationMonths as varchar(2)) + ' Month ' as product_category,
	count(*) as orders
into #good_products
from Typhoon.dbo.Typhoon_Order_Items_RollUp as toir WITH(NOLOCK)
	inner join Typhoon.dbo.Typhoon_Date as td WITH(NOLOCK) on td.DateKey = toir.[OrderDate]
	inner join Typhoon.dbo.Typhoon_Customer_Rollup as tcr WITH(NOLOCK) on tcr.Customer = toir.Customer
	inner join Typhoon.dbo.Typhoon_Campaigns as tc WITH(NOLOCK) on tc.Campaign = tcr.Campaign
	inner join Typhoon.dbo.Typhoon_Products p on p.Product = toir.Product
where 
	p.IsSubscription = 1
	and [Is Trial Product] = 1  --allow the upgrade from free account non primary product in
	and (toir.businesspartner = 0 or toir.businesspartner = 11)
group by
	toir.[Is Trial Product],
	toir.SubscriptionDurationMonths,
	toir.[Subscription Price],
	case when [Is Trial Product] = 1 then 'Trial' else 'HO' end + ' ' + cast(toir.SubscriptionDurationMonths as varchar(2)) + ' Month ' + cast(toir.[Subscription Price] as varchar(10)),
	case when [Is Trial Product] = 1 then 'Trial' else 'HO' end + ' ' + cast(toir.SubscriptionDurationMonths as varchar(2)) + ' Month '
having 
	count (*) >= 10000   --product cutoff
order by 
	toir.[Is Trial Product],
	toir.SubscriptionDurationMonths,
	toir.[Subscription Price]  



-- create the temp1 table so the code runs the first time.  

if  object_id('tempdb..##temp1') is not null 
drop table tempdb..##temp1 ;   
select top 1
	[month],
	[Net Sale Count],
	count(*) as sales,
	max([Net Sale Count]) over (partition by [month]) as max_sale_count,
	max(subscriptionDurationMonths) as duration --max is ok, the only time there are multiple durations for a product is when one is incorrectly 0  
into ##temp1
from #fact
where
	test = 1
group by
	[month],
	[Net Sale Count]
order by
	[month],
	[Net Sale Count]


-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- declare variables and cursors



declare product_cat_cursor cursor for 
select distinct 'and Product_Category_Fine = ''' + Product_Category_Fine + ''''
from #good_products 





--start cursor loop to get renewal rates for all the cohorts 

print 'Begin Cursor Loop'


open product_cat_cursor

declare @product_cat varchar(255)
fetch next from product_cat_cursor into @product_cat



while @@fetch_status = 0
begin
	print @product_cat

	--where conditions to loop through  
	declare campaign_cursor cursor for 

	select distinct 'and ChannelName = ''' + rtrim(ChannelName) + ''' ' + 'and OriginName = ''' + rtrim(OriginName) + ''' ' + rtrim(case when CampaignGroupName is null then '' else 'and CampaignGroupName = ''' + CampaignGroupName + ''' ' end) + 'and Campaign = ''' + rtrim(Campaign) + '''' from #good_campaigns
	union
	select distinct 'and ChannelName = ''' + rtrim(ChannelName) + ''' ' + 'and OriginName = ''' + rtrim(OriginName) + ''' ' + 'and CampaignGroupName = ''' + rtrim(CampaignGroupName) + '''' from #good_campaigns
	union
	select distinct 'and ChannelName = ''' + rtrim(ChannelName) + ''' ' + 'and OriginName = ''' + rtrim(OriginName) + '''' from #good_campaigns
	union
	select distinct 'and ChannelName = ''' + rtrim(ChannelName) + '''' from #good_campaigns
	union
	select distinct ' '

	open campaign_cursor

	declare @campaign varchar(1000)
	fetch next from campaign_cursor into @campaign

	while @@fetch_status = 0
	begin
		print @product_cat + @campaign

		--get the sales data by net sale count for the specified cohort 
		
		declare @sql varchar(8000)
		set  @sql = 
	
		'if  object_id(''tempdb..##temp1'') is not null 
		drop table tempdb..##temp1 ;   
		select
			[month],
			[Net Sale Count],
			count(*) as sales,
			max([Net Sale Count]) over (partition by [month]) as max_sale_count,
			max(subscriptionDurationMonths) as duration --max is ok, the only time there are multiple durations for a product is when one is incorrectly 0  
		into ##temp1
		from #fact
		where
			test = 1
			' + @product_cat + '
			' + @campaign + '
		group by
			[month],
			[Net Sale Count]
		order by
			[month],
			[Net Sale Count]'
			
		exec (@sql)


		-- get the running total sales (self join)


		if  object_id('tempdb..#temp2') is not null 
		drop table tempdb..#temp2 ;   
		select
			t.[month],
			t.[Net Sale Count],
			t.duration,
			dense_rank() over (order by t.[month] desc) - 2 as month_number,
			cast(cast(datepart(year, getdate()) as char(4)) + cast(datepart(month, getdate()) as varchar(2)) as int) as todays_month,
			datediff(month, cast(substring(cast(t.[month] as char(6)), 1, 4) + '-' + substring(cast(t.[month] as char(6)), 5, 2) + '-01' as date), getdate()) - 1 as month_number3,  --use months from today's month now 
			sum(t2.[Sales]) as cumulative_sales
		into #temp2 
		from ##temp1 t
			inner join ##temp1 t2 on t2.[month] = t.[month] and t2.[Net Sale Count] >= t.[Net Sale Count]
		group by
			t.[month],
			t.[Net Sale Count],
			t.duration
		order by
			t.[month],
			t.[Net Sale Count]


		delete from #temp2 where month_number3 <= 0



		-- aggregate by net sale count for 3(adjustable) months rolling (self join)


		if  object_id('tempdb..#temp3') is not null 
		drop table tempdb..#temp3 ;   
		select
			t.[month],
			t.[Net Sale Count],
			t.cumulative_sales,
			t.month_number3 as month_number,
			t.duration,
			sum(t2.cumulative_sales) as grouped_cumulative_sales
		into #temp3
		from #temp2 t
			left join #temp2 t2 on t2.month_number3 >= t.month_number3 and t2.month_number3 <= t.month_number3 + @rolling_months - 1 and t2.[Net Sale Count] = t.[Net Sale Count]
		group by
			t.[month],
			t.[Net Sale Count],
			t.cumulative_sales,
			t.month_number3,
			t.duration
		order by
			t.[month],
			t.[Net Sale Count],
			t.cumulative_sales
			
			
		delete from #temp3 where grouped_cumulative_sales <= @data_cutoff --new code to remove tiny cohorts now 


		-- calculate the renewal rates (self join)


		if  object_id('tempdb..#temp4') is not null 
		drop table tempdb..#temp4 ;   
		select
			t.[month] as order_month,
			t.[Net Sale Count],
			t.cumulative_sales,
			t.grouped_cumulative_sales,
			t.month_number as month_number,
			t.duration,
			cast(t.cumulative_sales as decimal)/cast(t2.cumulative_sales as decimal) as renewal_rate,
			cast(t.grouped_cumulative_sales as decimal)/cast(t2.grouped_cumulative_sales as decimal) as rolling_renewal_rate,
			max(t.[Net Sale Count]) over (partition by t.[month]) as max_sale_count
		into #temp4
		from #temp3 t
			left join #temp3 t2 on t2.[month] = t.[month] and t2.[Net Sale Count] = t.[Net Sale Count] - 1
		order by
			t.[month],
			t.[Net Sale Count]

		delete from #temp4 where renewal_rate is null 


		--dense rank to force month_number to be sequential even if there are gaps 
		

		if  object_id('tempdb..#temp45') is not null 
		drop table tempdb..#temp45 ;
		select distinct
			t.order_month,
			t.[Net Sale Count],
			t.cumulative_sales,
			t.grouped_cumulative_sales,
			t.month_number as old_month_number,
			dense_rank() over (order by t.order_month desc) as month_number,
			t.duration,
			t.renewal_rate,
			t.rolling_renewal_rate,
			t.max_sale_count	
		into #temp45 
		from #temp4 t
		order by t.order_month, t.[net sale count]



		-- for each period, use the most recent data possible for the renewal rate 

		delete from #temp45 where ([Net Sale Count]-1)*duration +1 != month_number -- genius  



		if  object_id('tempdb..#temp5') is not null 
		drop table tempdb..#temp5 ;
		select 
			order_month,
			[Net Sale Count],
			cumulative_sales,
			grouped_cumulative_sales,
			month_number,
			duration,
			renewal_rate,
			rolling_renewal_rate,
			max_sale_count,
			min(case when rolling_renewal_rate >= 1 then [net sale count] else 9999 end) over (partition by 1) as first_bad_renewal
		into #temp5
		from #temp45


		insert into #renewal_rates
		select 
			substring(@product_cat, charindex('''', @product_cat, 1) + 1, len(@product_cat) - charindex('''', @product_cat, 1) - 1), 
			case when @campaign = ' ' then 'Overall'  else substring(@campaign, 5, charindex('=',@campaign, 1) - 5) end,
			case when @campaign = ' ' then 'Overall'  else substring(@campaign, 4, len(@campaign)-3) end,
			order_month,
			[Net Sale Count],
			cumulative_sales,
			grouped_cumulative_sales,
			month_number,
			duration,
			renewal_rate,
			rolling_renewal_rate,
			max_sale_count
		from #temp5
		where [net sale count] < first_bad_renewal

		fetch next from campaign_cursor into @campaign	
		
	end
	close campaign_cursor
	deallocate campaign_cursor	

	fetch next from product_cat_cursor into @product_cat	

end

close product_cat_cursor
deallocate product_cat_cursor

drop table ##temp1
drop table #temp2
drop table #temp3
drop table #temp4
drop table #temp5
drop table #fact



--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--II.  Get the multiplier for additional revenue coming in from OTO/Partner Subs/Reports  ----------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- create an initial table with all the primary subsciption product sales 


if  object_id('tempdb..#mult1') is not null 
drop table tempdb..#mult1 ;   
select distinct 
	tti.Customer,
	case when [Is Trial Product] = 1 then 'Trial' else 'HO' end + ' ' + cast(tti.SubscriptionDurationMonths as varchar(2)) + ' Month ' + cast(tti.[Subscription Price] as varchar(10)) as product_category_fine,
	c.campaign,
	c.channelname,
	c.campaigngroupname,
	c.OriginName + 
		case when ChannelName = 'SEM' and Originname not like '%Mobile%' then  --something wrong here also 
			case when CHARINDEX('_d', msid, 1) = 0 then ' Desktop' else 
				case	when SUBSTRING(msid,CHARINDEX('_d', msid, 1) + 2, 1) = 'c' then ' Desktop' 
						when SUBSTRING(msid,CHARINDEX('_d', msid, 1) + 2, 1) = 't' then ' Desktop' 
						when SUBSTRING(msid,CHARINDEX('_d', msid, 1) + 2, 1) = 'm' then ' Mobile' 
						when MSID is null then ' Desktop'  
						else ' error' end 
				end
		else '' end as OriginName,
	tti.orderdatestamp,
	sum(tti.[Net Sales]) as Total_Net_Sales
into #mult1
from Typhoon.dbo.Typhoon_Transaction_Items as tti WITH(NOLOCK)
	inner join Typhoon.dbo.Typhoon_Campaigns as c WITH(NOLOCK) on c.Campaign = tti.CustomerCampaign
	inner join Typhoon.dbo.Typhoon_Products p on p.Product = tti.Product
where
	[Is Trial Product] = 1
	and [Is Subscription Product] = 1
	and (tti.businesspartner = 0 or tti.businesspartner = 11)
	and tti.Customer <> -1
	and tti.orderdatestamp between dateadd(month, -25, getdate()) and dateadd(month, -13, getdate()) -- 11 months of data  
	and tti.transactiondatestamp between orderdatestamp and dateadd(month, 13, orderdatestamp) -- keep only the first 13 months of renewal data  
group by
	tti.Customer,
	case when [Is Trial Product] = 1 then 'Trial' else 'HO' end + ' ' + cast(tti.SubscriptionDurationMonths as varchar(2)) + ' Month ' + cast(tti.[Subscription Price] as varchar(10)),
	c.campaign,
	c.channelname,
	c.campaigngroupname,
	c.OriginName + 
		case when ChannelName = 'SEM' and Originname not like '%Mobile%' then  --something wrong here also 
			case when CHARINDEX('_d', msid, 1) = 0 then ' Desktop' else 
				case	when SUBSTRING(msid,CHARINDEX('_d', msid, 1) + 2, 1) = 'c' then ' Desktop' 
						when SUBSTRING(msid,CHARINDEX('_d', msid, 1) + 2, 1) = 't' then ' Desktop' 
						when SUBSTRING(msid,CHARINDEX('_d', msid, 1) + 2, 1) = 'm' then ' Mobile' 
						when MSID is null then ' Desktop'  
						else ' error' end 
				end
		else '' end,
	tti.orderdatestamp
	
-- only check for customers that had a primary order with actual net sales  

delete from #mult1 where total_net_sales = 0

-- keep only the first product 


if  object_id('tempdb..#mult2') is not null 
drop table tempdb..#mult2 ;  
select 
	*,
	row_number() over (partition by Customer order by Product_Category_Fine) as row,
	count(customer) over (partition by 	Product_Category_Fine, campaign, channelname, campaigngroupname, originname) as customer_count
into #mult2
from #mult1
order by 
	customer, 
	Product_Category_Fine

delete from #mult2 where customer_count < @mult_customer_cutoff -- keep only cohorts with enough data to make a reasonable calculation  
delete from #mult2 where row > 1

-- get the other net sales data to caluculate the multiplier  


if  object_id('tempdb..#mult3') is not null 
drop table tempdb..#mult3 ;  
select
	m.Product_Category_Fine,
	m.campaign,
	m.channelname,
	m.campaigngroupname,
	m.originname,
	m.orderdatestamp,
	sum(m.total_net_sales) primary_net_sales,
	sum([Net Sales]) other_net_sales
into #mult3
from #mult2 m
	left join Typhoon.dbo.typhoon_transaction_items as tti WITH(NOLOCK) on 
		tti.customer = m.customer
		and tti.orderdatestamp >= m.orderdatestamp
		and tti.transactiondatestamp between m.orderdatestamp and dateadd(month, 13, m.orderdatestamp) 
		and ((tti.[Is Subscription Product] <> 1
		and tti.[Is Trial Product] <> 1)  --allow the upgrade from free account non primary product in
		or tti.businesspartner <> 0)
group by
	m.Product_Category_Fine,
	m.campaign,
	m.channelname,
	m.campaigngroupname,
	m.originname,
	m.orderdatestamp,
	m.total_net_sales


--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

--use the cursor to create the aggregations and get the multiplier on a cohort level  


if  object_id('tempdb..#multiplier') is not null 
drop table tempdb..#multiplier ;  
create table #multiplier (product_category_fine varchar(128), aggregation_level varchar (128), aggregation_value varchar(255), primary_net_sales money,  other_net_sales money)

--pre-create the table so the query can run  

if  object_id('tempdb..##aggregate') is not null 
drop table tempdb..##aggregate
select
	sum(primary_net_sales) as primary_net_sales,
	sum(other_net_sales) as other_net_sales
into ##aggregate
from #mult3

-- declare variables and cursors


declare product_cat_cursor2 cursor for 
select distinct 'and Product_Category_Fine = ''' + Product_Category_Fine + ''''
from #good_products 


open product_cat_cursor2

declare @product_cat2 varchar(255)
fetch next from product_cat_cursor2 into @product_cat2


--loop through all the cohorts 

while @@fetch_status = 0
begin
	print @product_cat2

	--where conditions to loop through  
	declare campaign_cursor2 cursor for 

	select distinct 'and ChannelName = ''' + rtrim(ChannelName) + ''' ' + 'and OriginName = ''' + rtrim(OriginName) + ''' ' + rtrim(case when CampaignGroupName is null then '' else 'and CampaignGroupName = ''' + CampaignGroupName + ''' ' end) + 'and Campaign = ''' + rtrim(Campaign) + '''' from #good_campaigns
	union
	select distinct 'and ChannelName = ''' + rtrim(ChannelName) + ''' ' + 'and OriginName = ''' + rtrim(OriginName) + ''' ' + 'and CampaignGroupName = ''' + rtrim(CampaignGroupName) + '''' from #good_campaigns
	union
	select distinct 'and ChannelName = ''' + rtrim(ChannelName) + ''' ' + 'and OriginName = ''' + rtrim(OriginName) + '''' from #good_campaigns
	union
	select distinct 'and ChannelName = ''' + rtrim(ChannelName) + '''' from #good_campaigns
	union
	select distinct ' '

	open campaign_cursor2

	declare @campaign2 varchar(1000)
	fetch next from campaign_cursor2 into @campaign2

	while @@fetch_status = 0
	begin
		print @product_cat2 + @campaign2


	
		declare @sql2 varchar(8000)
		set  @sql2 = 
		'
		if  object_id(''tempdb..##aggregate'') is not null 
		drop table tempdb..##aggregate
		select
			sum(primary_net_sales) as primary_net_sales,
			sum(other_net_sales) as other_net_sales
		into ##aggregate
		from #mult3
		where 
			1 = 1 
			' + @product_cat2 + '
			' + @campaign2 + '
		'
		exec(@sql2)
	
		insert into #multiplier
		select
			substring(@product_cat2, charindex('''', @product_cat2, 1) + 1, len(@product_cat2) - charindex('''', @product_cat2, 1) - 1), 
			case when @campaign2 = ' ' then 'Overall'  else substring(@campaign2, 5, charindex('=',@campaign2, 1) - 5) end,
			case when @campaign2 = ' ' then 'Overall'  else substring(@campaign2, 4, len(@campaign2)-3) end,
			primary_net_sales,
			other_net_sales
		from ##aggregate
				
		fetch next from campaign_cursor2 into @campaign2	
		
	end
	close campaign_cursor2
	deallocate campaign_cursor2

	fetch next from product_cat_cursor2 into @product_cat2	

end

close product_cat_cursor2
deallocate product_cat_cursor2


delete from #multiplier where primary_net_sales is null

drop table #mult1
drop table #mult2
drop table #mult3



/*--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--III.  Create the Default Rate, and apply the renewal rates to calculate the LTV at a product level ---------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

--get the renewal rates with data removed and including price.  This step also removes data issues.  


if  object_id('tempdb..#renewal_rates2') is not null 
drop table tempdb..#renewal_rates2
select 
	rr.*,
	p.[subscription price] as price,
	aggregation_value as campaign_detail,
	p.[subscription price] * @fees as fee_price,
	min(case when rr.grouped_cumulative_sales <= @data_cutoff then net_sale_count else 1000 end) over (partition by rr.product_category_fine, rr.aggregation_value) as min_Month,
	min(rr.net_sale_count) over (partition by rr.product_category_fine, rr.aggregation_value) as min_Month_overall,
	count(net_sale_count) over (partition by rr.product_category_fine, rr.aggregation_value) as renewal_count,
	max(net_sale_count) over (partition by rr.product_category_fine, rr.aggregation_value) as max_renewal_count
into #renewal_rates2
from #renewal_rates   rr
	inner join #good_products p on p.product_category_fine = rr.product_category_fine
order by 
	rr.product_category_fine, 
	campaign_detail, net_sale_count desc


delete from #renewal_rates2
where net_sale_count >= min_month  --make sure there are no gaps in the data

delete from #renewal_rates2 
where net_sale_count > renewal_count


--need to get the default rate in here (make a default rate table), also to get 60 periods for all the products---------------------------------------------------------------------------
--create a 61 period dimension table by product category


if  object_id('tempdb..#number') is not null 
drop table tempdb..#number
create table #number(period smallint)

declare @num smallint
set @num = 1

while @num <= 61
begin
	insert into #number select @num
	set @num = @num+1
end	

if  object_id('tempdb..#product') is not null 
drop table tempdb..#product
select distinct product_category_fine, duration into #product from #renewal_rates2


if  object_id('tempdb..#dimension') is not null 
drop table tempdb..#dimension
select * 
into #dimension
from #product
cross join #number



--join to get the default renewal rates for all 61 periods

if  object_id('tempdb..#default_rates') is not null 
drop table tempdb..#default_rates
select 
	n.product_category_fine, 
	n.period,
	max(n.period * case when rolling_renewal_rate is not null then 1 else 0 end) over (partition by n.product_category_fine) as max_period,
	rr.rolling_renewal_rate,
	1 as is_actual
into #default_rates 
from #dimension n
	left join #renewal_rates2 rr  on 
		n.period = rr.net_sale_count 
		and campaign_detail = 'overall'
		and n.product_category_fine = rr.product_category_fine
		and grouped_cumulative_sales >= @data_cutoff
order by 
	product_category_fine, 
	period





--get the last renewal rate for each  


if  object_id('tempdb..#default_rates2') is not null 
drop table tempdb..#default_rates2
select 
	product_category_fine, 
	period,
	max_period,
	rolling_renewal_rate,
	is_actual,
	max(case when period = max_period then rolling_renewal_rate else 0 end) over (partition by product_category_fine) as final_renewal_rate
into #default_rates2
from #default_rates
group by 
	product_category_fine, 
	period,
	max_period,
	rolling_renewal_rate,
	is_actual	


update  #default_rates2  set is_actual = 0 
where rolling_renewal_rate is null

update #default_rates2  set rolling_renewal_rate = final_renewal_rate -- the data goes out based on last renewal rate
where rolling_renewal_rate is null


--self join to get the percent changes in the default rate 

if  object_id('tempdb..#default_rates3') is not null 
drop table tempdb..#default_rates3
select 
	rr.*,
	rr.rolling_renewal_rate / rr2.rolling_renewal_rate as pct_change
into #default_rates3
from #default_rates2 rr
	left join #default_rates2 rr2 on 
		rr2.product_category_fine = rr.product_category_fine 
		and rr2.period + 1 = rr.period 



--update this to manually set some default rates 

if  object_id('tempdb..#default_rates4') is not null 
drop table tempdb..#default_rates4
select
	a.product_category_fine,
	a.period,
	a.max_period,
	case	when a.product_category_fine in ('Trial 1 Month 6.95', 'Trial 1 Month 7.99', 'Trial 1 Month 8.95', 'Trial 1 Month 9.95') then b.rolling_renewal_rate
			else a.rolling_renewal_rate end as rolling_renewal_rate,
	a.is_actual,
	case	when a.product_category_fine in ('Trial 1 Month 6.95', 'Trial 1 Month 7.99', 'Trial 1 Month 8.95', 'Trial 1 Month 9.95') then b.final_renewal_rate
			else a.final_renewal_rate end as final_renewal_rate,
	case	when a.product_category_fine in ('Trial 1 Month 6.95', 'Trial 1 Month 7.99', 'Trial 1 Month 8.95', 'Trial 1 Month 9.95') then b.pct_change
			else a.pct_change end as pct_change
into #default_rates4	
from #default_rates3 a
left join #default_rates3 b on a.period = b.period and b.product_category_fine = 'Trial 1 Month 7.95'




--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- 3 level dimension table 


if  object_id('tempdb..#dimensionthree') is not null 
drop table tempdb..#dimensionthree
select * 
into #dimensionthree
from #dimension a
	cross join (select distinct campaign_detail from #renewal_rates2) as b


-- join every thing to this dimension table 



if  object_id('tempdb..#renewal_rates3') is not null 
drop table tempdb..#renewal_rates3
select 
	dm.*,
	d.rolling_renewal_rate as default_rate,
	d.pct_change,
	rr.rolling_renewal_rate,
	p.[subscription price],
	p.[subscription price] * @fees as fee_price,
	case when rr.rolling_renewal_rate is null then 0 else 1 end as is_actual
into #renewal_rates3
from #dimensionthree dm
	left join #default_rates4 d on 
		d.product_category_fine = dm.product_category_fine
		and d.period = dm.period
	left join #renewal_rates2 rr on 
		rr.product_category_fine = dm.product_category_fine
		and rr.net_sale_count = dm.period
		and rr.campaign_detail = dm.campaign_detail
		and rr.net_sale_count < rr.min_month --filters out low data rewewal rates 
		and rr.min_month_overall = 1 --make sure all cohorts start on the first month
	inner join #good_products p on p.product_category_fine = dm.product_category_fine




--created the last two rows blend and the maximum renewal rate (based on the default rate)  

if  object_id('tempdb..#renewal_rates4') is not null 
drop table tempdb..#renewal_rates4
select 
	rr.*,
	rr2.rolling_renewal_rate as previous_rolling_renewal_rate,
	(rr.rolling_renewal_rate + rr2.rolling_renewal_rate * rr.pct_change)/2 as last_two_blend,
	max(rr.period * case when rr.rolling_renewal_rate is not null then 1 else 0 end) over (partition by rr.product_category_fine, rr.campaign_detail) as max_period,
	max(rr.default_rate) over (partition by rr.product_category_fine, rr.campaign_detail) as max_renewal, -- renewal rates will be limited by the highest default rate
	cast (0 as float) as full_renewal_rate
into #renewal_rates4 
from #renewal_rates3 rr
	left join #renewal_rates3 rr2 on 
		rr2.product_category_fine = rr.product_category_fine
		and rr2.campaign_detail = rr.campaign_detail
		and rr2.period = rr.period - 1
order by rr.product_category_fine, rr.campaign_detail, rr.period

delete from #renewal_rates4 --remove any entries with no renewal rates  
where max_period = 0


update #renewal_rates4 
set full_renewal_rate = rolling_renewal_rate
where period < max_period

update #renewal_rates4 
set full_renewal_rate = coalesce(last_two_blend, rolling_renewal_rate)
where period = max_period



-- loop through to apply the actual default rates to fill down the renewal rates when there is no data  


if  object_id('tempdb..##apply_default62') is not null 
drop table tempdb..##apply_default62
if  object_id('tempdb..##apply_default0') is not null 
drop table tempdb..##apply_default0

select distinct * into ##apply_default0 from #renewal_rates4 

declare @period int
declare @next_period int

set @period = 0
set @next_period = 1

while @period <= 61
begin

	print 'period ' + ltrim(@period)

	declare @sql3 varchar(8000)
	set  @sql3 = 
				
		'select
		d.product_category_fine,
		d.period,
		d.campaign_detail,
		d.default_rate,
		d.pct_change,
		d.rolling_renewal_rate,
		d.[subscription price],
		d.fee_price,
		d.is_actual,
		d.duration,
		d.previous_rolling_renewal_rate,
		d.last_two_blend,
		d.max_period,
		d.max_renewal,
		case when case when d.full_renewal_Rate = 0 then coalesce(d2.full_renewal_rate, 0)* d.pct_change else d.full_renewal_rate end >= d.max_renewal then d.max_renewal else
		case when d.full_renewal_Rate = 0 then coalesce(d2.full_renewal_rate, 0)* d.pct_change else d.full_renewal_rate end end as full_renewal_rate -- apply the actual 
	into ##apply_default' + ltrim(@next_period) + '
	from ##apply_default' + ltrim(@period) + ' d
		left join ##apply_default' + ltrim(@period) + ' d2 on
			d2.product_category_fine = d.product_category_fine
			and d2.period = d.period - 1
			and d2.campaign_detail = d.campaign_detail
			and d2.period = ' + ltrim(@next_period) +'
	
	drop table ##apply_default' + ltrim(@period)
	
	exec (@sql3)
	
	set @period = @period + 1
	set @next_period = @next_period + 1

end



-- remove the blended renewal rate from the full_renewal_rate 

update ##apply_default62
set full_renewal_rate = rolling_renewal_rate
where is_actual = 1


drop table #renewal_rates2
drop table #renewal_rates3
drop table #renewal_rates4


--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

--self join to get a cumulative renewal series  -- self join and trick to get a product() function  

if  object_id('tempdb..#renewal_calc') is not null 
drop table tempdb..#renewal_calc
select 
	rr.product_category_fine,
	rr.campaign_detail,
	rr.period,
	rr.[subscription price],
	rr.fee_price,
	rr.rolling_renewal_rate,
	rr.is_actual,
	rr.duration,
	rr.max_period,
	rr.default_rate,
	rr.pct_change,
	rr.full_renewal_rate,
	power(cast(1+@discount_rate as float),cast(rr.duration as float)/12) - 1 as period_discount_rate,
	power(10.000000000000, sum(log10(rr2.full_renewal_rate))) as cumulative_rolling_renewal  --algebra trick to get the product of this column
into #renewal_calc
from ##apply_default62 rr
	left join ##apply_default62 rr2 on 
		rr2.product_category_fine = rr.product_category_fine 
		and rr2.campaign_detail = rr.campaign_detail
		and rr2.period <= rr.period
group by 
	rr.product_category_fine,
	rr.campaign_detail,
	rr.period,
	rr.[subscription price],
	rr.fee_price,
	rr.rolling_renewal_rate,
	rr.is_actual,
	rr.duration,
	rr.max_period,
	rr.default_rate,
	rr.pct_change,
	rr.full_renewal_rate
order by 
	rr.product_category_fine, 
	rr.campaign_detail, 
	rr.period



-- multiplication step to get the dollar values and apply discounting and add the perpetuity  



if  object_id('tempdb..#renewal_calc2') is not null 
drop table tempdb..#renewal_calc2
select 
	*,
	cumulative_rolling_renewal * fee_price as cashflow,
	(cumulative_rolling_renewal * fee_price)/power(1+period_discount_rate, period - 1) as discounted_cashflow,
	case when period = 61 then (cumulative_rolling_renewal * fee_price) * (1/(1-full_renewal_rate)-1) else 0 end as perpetuity,
	case when period = 61 then ((cumulative_rolling_renewal * fee_price)/power(1+period_discount_rate, period - 1)) * (1/(1-full_renewal_rate)-1) else 0 end as discounted_perpetuity
into #renewal_calc2
from #renewal_calc 



-- sum everything up together and join with the multipliers to get the total ltv for each cohort  



if  object_id('tempdb..#renewal_calc3') is not null 
drop table tempdb..#renewal_calc3
select 
	rr.product_category_fine,
	ltrim(campaign_detail) as campaign_detail,
	duration,
	fee_price,
	max(case when period = 1 then full_renewal_rate else 0 end) as billthrough_rate,
	max(coalesce(m.primary_net_sales, m2.primary_net_sales, m3.primary_net_sales, m4.primary_net_sales)) as primary_net_sales,
	max(coalesce(m.other_net_sales, m2.other_net_sales, m3.other_net_sales, m4.other_net_sales)) as other_net_sales,
	max(coalesce(m.other_net_sales, m2.other_net_sales, m3.other_net_sales, m4.other_net_sales)/coalesce(m.primary_net_sales, m2.primary_net_sales, m3.primary_net_sales, m4.primary_net_sales)) as multiplier,
	sum(cashflow + perpetuity) as ltr,
	sum(cashflow * is_actual + perpetuity) as actual_ltr,
	sum(discounted_cashflow + discounted_perpetuity) as discounted_ltr,
	sum(discounted_cashflow * is_actual + discounted_perpetuity) as actual_discounted_ltr,
	sum(case when period <= 1/(cast(duration as float)/12) * 4 + 1 then cashflow else 0 end) as four_year_ltr,  -- need to check that this is right (fixed now)
	sum(case when period <= 1/(cast(duration as float)/12) * 4 + 1 then cashflow else 0 end * is_actual) as actual_four_year_ltr,
	sum(case when period <= 1/(cast(duration as float)/12) * 4 + 1 then discounted_cashflow else 0 end) as discounted_four_year_ltr,
	sum(case when period <= 1/(cast(duration as float)/12) * 4 + 1 then discounted_cashflow else 0 end * is_actual) as actual_discounted_four_year_ltr
into #renewal_calc3
from #renewal_calc2 rr
		inner join #good_products p on p.product_category_fine = rr.product_category_fine
		left join #multiplier m on 
			m.product_category_fine = rr.product_category_fine
			and m.aggregation_value = rr.campaign_detail
		left join #multiplier m2 on -- use the channel level if campaign level doesn't work  
			m2.product_category_fine = rr.product_category_fine
			and substring(m2.aggregation_value, 1, charindex(char(39), m2.aggregation_value, charindex(char(39), m2.aggregation_value)+1)) = substring(rr.campaign_detail, 1, charindex(char(39), rr.campaign_detail, charindex(char(39), rr.campaign_detail)+1)) 
			and charindex(char(39), m2.aggregation_value, charindex(char(39), m2.aggregation_value)+1) = len(m2.aggregation_value)
		left join #multiplier m3 on -- otherwise use overall level  
			m3.product_category_fine = rr.product_category_fine
			and m3.aggregation_value = 'Overall'
		left join #multiplier m4 on -- otherwise use other priced product!
			substring(m4.product_category_fine, 1, CHARINDEX('month', m4.product_category_fine, 1 )+5) = substring(rr.product_category_fine, 1, CHARINDEX('month', rr.product_category_fine, 1 )+5)
			and m4.aggregation_value = 'Overall'
			and rtrim(substring(m4.product_category_fine, CHARINDEX('month', m4.product_category_fine, 1 )+6, 10 )) in ('39.95', '7.95', '19.95') -- pick the dominant price to use (if more than one have an aggregation value)  			
group by
	rr.product_category_fine,
	campaign_detail,
	duration,
	fee_price







-- final step to get the LTR with multipliers and the pct from historical  


if  object_id('tempdb..#ltr') is not null 
drop table tempdb..#ltr
select
	product_category_fine,
	campaign_detail,
	duration,
	fee_price,
	billthrough_rate,
	1+multiplier as multiplier,
	ltr*(1+multiplier) as ltr,
	discounted_ltr*(1+multiplier) as discounted_ltr,
	four_year_ltr * (1+multiplier) as four_year_ltr,
	discounted_four_year_ltr *(1+multiplier) as discounted_four_year_ltr,
	actual_ltr/ltr as ltr_pct_hist,
	actual_discounted_ltr/discounted_ltr  as discounted_ltr_pct_hist,
	actual_four_year_ltr/four_year_ltr as four_year_ltr_pct_hist,
	actual_discounted_four_year_ltr/discounted_four_year_ltr as discounted_four_year_ltr_pct_hist
into #ltr
from #renewal_calc3



delete from #ltr where discounted_ltr_pct_hist < @pct_cutoff



drop table #renewal_calc
drop table #renewal_calc2
drop table #renewal_calc3


-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--4. Apply a roll up logic to create an LTV value for all possible cohorts 
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


--campaign group table (fix this for the new orgins created for sem desktop and mobile) 


if  object_id('tempdb..#adwords_campaigns') is not null 
drop table tempdb..#adwords_campaigns
select distinct
	campaign,
	campaigngroupname,
	originname,
	channelname
into #adwords_campaigns
from typhoon.dbo.typhoon_campaigns tc 
where channelname <> 'SEM'

insert into #adwords_campaigns
select distinct
	campaign,
	campaigngroupname,
	replace(originname, ' Mobile', '') + ' Desktop',
	channelname
from typhoon.dbo.typhoon_campaigns tc 
where channelname = 'SEM'

insert into #adwords_campaigns
select distinct
	campaign,
	campaigngroupname,
	replace(originname, ' Mobile', '') + ' Mobile',
	channelname
from typhoon.dbo.typhoon_campaigns tc 
where channelname = 'SEM'


if  object_id('tempdb..#campaigns') is not null 
drop table tempdb..#campaigns
select *
into #campaigns
from
	(
	select distinct 
		'ChannelName = ''' + rtrim(ChannelName) + ''' ' + 'and OriginName = ''' + rtrim(OriginName) + ''' ' + rtrim(case when CampaignGroupName is null then '' else 'and CampaignGroupName = ''' + CampaignGroupName + ''' ' end) + 'and Campaign = ''' + rtrim(Campaign) + '''' as campaign_detail,
		ChannelName,
		OriginName,
		CampaignGroupName,
		Campaign
	from #adwords_campaigns
	union
	select distinct 
		'ChannelName = ''' + rtrim(ChannelName) + ''' ' + 'and OriginName = ''' + rtrim(OriginName) + ''' ' + 'and CampaignGroupName = ''' + rtrim(CampaignGroupName) + '''',
		ChannelName,
		OriginName,
		CampaignGroupName,
		'Overall'
	from #adwords_campaigns
	where campaigngroupname is not null
	union
	select distinct 
		'ChannelName = ''' + rtrim(ChannelName) + ''' ' + 'and OriginName = ''' + rtrim(OriginName) + '''',
		ChannelName,
		OriginName,
		'Overall',
		'Overall'
	from #adwords_campaigns
	union
	select distinct 
		'ChannelName = ''' + rtrim(ChannelName) + '''',
		ChannelName,
		'Overall',
		'Overall',
		'Overall'
	from #adwords_campaigns
	union
	select distinct 
		'Overall',
		'Overall',
		'Overall',
		'Overall',
		'Overall'
	) as g



-- add the individual campaign fields to the ltr table  

if  object_id('tempdb..#ltr2') is not null 
drop table tempdb..#ltr2
select 
	l.*,
	ChannelName,
	OriginName,
	CampaignGroupName,
	Campaign,
	case	when ChannelName = 'Overall' then 'Product Level'
			when OriginName = 'Overall' then 'Channel Level'
			when CampaignGroupName = 'Overall' then 'Origin Level'
			when Campaign = 'Overall' then 'Campaign Group Level'
			else 'Campaign Level' end as Aggregation_level
into #ltr2
from #ltr l
left join #campaigns c on ltrim(c.campaign_detail) = ltrim(l.campaign_detail)
order by product_category_fine, l.campaign_detail


--cross join with products to get the full dimension table 

if  object_id('tempdb..#campaign_product') is not null 
drop table tempdb..#campaign_product
select * 
into #campaign_product
from #campaigns
cross join (select distinct product_category_fine, subscriptiondurationmonths as duration, [subscription price] from #good_products) as p


--rollup logic now - cohorts without enough data are rolled up to the next level  



if  object_id('tempdb..#product_ltr') is not null 
drop table tempdb..#product_ltr
select
	cp.product_category_fine,
	cp.campaign_detail,
	cp.duration,
	cp.[subscription price],
	cp.ChannelName,
	cp.OriginName,
	cp.CampaignGroupName,
	cp.Campaign,
	coalesce(l.billthrough_rate,l2.billthrough_rate,l3.billthrough_rate,l4.billthrough_rate,l5.billthrough_rate) as billthrough_rate,
	coalesce(l.ltr,l2.ltr,l3.ltr,l4.ltr,l5.ltr) as ltr,
	coalesce(l.discounted_ltr,l2.discounted_ltr,l3.discounted_ltr,l4.discounted_ltr,l5.discounted_ltr) as discounted_ltr,
	coalesce(l.four_year_ltr,l2.four_year_ltr,l3.four_year_ltr,l4.four_year_ltr,l5.four_year_ltr) as four_year_ltr,
	coalesce(l.discounted_four_year_ltr,l2.discounted_four_year_ltr,l3.discounted_four_year_ltr,l4.discounted_four_year_ltr,l5.discounted_four_year_ltr) as discounted_four_year_ltr,
	coalesce(l.ltr_pct_hist,l2.ltr_pct_hist,l3.ltr_pct_hist,l4.ltr_pct_hist,l5.ltr_pct_hist) as ltr_pct_hist,
	coalesce(l.discounted_ltr_pct_hist,l2.discounted_ltr_pct_hist,l3.discounted_ltr_pct_hist,l4.discounted_ltr_pct_hist,l5.discounted_ltr_pct_hist) as discounted_ltr_pct_hist,
	coalesce(l.four_year_ltr_pct_hist,l2.four_year_ltr_pct_hist,l3.four_year_ltr_pct_hist,l4.four_year_ltr_pct_hist,l5.four_year_ltr_pct_hist) as four_year_ltr_pct_hist,
	coalesce(l.discounted_four_year_ltr_pct_hist,l2.discounted_four_year_ltr_pct_hist,l3.discounted_four_year_ltr_pct_hist,l4.discounted_four_year_ltr_pct_hist,l5.discounted_four_year_ltr_pct_hist) as discounted_four_year_ltr_pct_hist,
	case	when l4.ltr is null then 'Product Level'
			when l3.ltr is null then 'Channel Level'
			when l2.ltr is null then 'Origin Level'
			when l.ltr is null then 'Campaign Group Level'
			else coalesce(l.aggregation_level, 'Campaign Level') end as "Aggregation Level"
into #product_ltr
from #campaign_product cp
	left join #ltr2 l on 
		l.product_category_fine = cp.product_category_fine	
		and l.channelname = cp.channelname
		and l.originname = cp.originname
		and l.campaigngroupname = cp.campaigngroupname
		and l.campaign = cp.campaign
	left join #ltr2 l2 on 
		l2.product_category_fine = cp.product_category_fine
		and l2.channelname = cp.channelname
		and l2.originname = cp.originname
		and l2.campaigngroupname = cp.campaigngroupname
		and l2.campaign = 'Overall'
	left join #ltr2 l3 on 
		l3.product_category_fine = cp.product_category_fine
		and l3.channelname = cp.channelname
		and l3.originname = cp.originname
		and l3.campaigngroupname = 'Overall'
		and l3.campaign = 'Overall'
	left join #ltr2 l4 on 
		l4.product_category_fine = cp.product_category_fine
		and l4.channelname = cp.channelname
		and l4.originname = 'Overall'
		and l4.campaigngroupname = 'Overall'
		and l4.campaign = 'Overall'
	left join #ltr2 l5 on 
		l5.product_category_fine = cp.product_category_fine
		and l5.channelname = 'Overall'
		and l5.originname = 'Overall'
		and l5.campaigngroupname = 'Overall'
		and l5.campaign = 'Overall'
order by 
 	cp.product_category_fine,
	cp.campaign_detail




-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--5. Create the final tables for typhoon
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


--table for excel ltv file 

if  object_id('tempdb..#typhoon_ltr_renewal_rates') is not null 
drop table tempdb..#typhoon_ltr_renewal_rates
select * 
into #typhoon_ltr_renewal_rates
from (
	select 
		rr.*, 
		'x' as placeholder,
		p.[subscription price],
		rr.aggregation_value as campaign_detail,
		coalesce(m.primary_net_sales, m2.primary_net_sales, m3.primary_net_sales, m4.primary_net_sales) as primary_net_sales,
		coalesce(m.other_net_sales, m2.other_net_sales, m3.other_net_sales, m4.other_net_sales) as other_net_sales,
		coalesce(m.other_net_sales, m2.other_net_sales, m3.other_net_sales, m4.other_net_sales)/coalesce(m.primary_net_sales, m2.primary_net_sales, m3.primary_net_sales, m4.primary_net_sales) as multiplier,
		min(case when rr.grouped_cumulative_sales <= @data_cutoff then net_sale_count else 1000 end) over (partition by rr.product_category_fine, rr.aggregation_value) as min_Month,
		min(rr.net_sale_count) over (partition by rr.product_category_fine, rr.aggregation_value) as min_Month_overall
	from #renewal_rates   rr
		inner join #good_products p on p.product_category_fine = rr.product_category_fine
		left join #multiplier m on 
			m.product_category_fine = rr.product_category_fine
			and m.aggregation_value = rr.aggregation_value
		left join #multiplier m2 on -- use the channel level if campaign level doesn't work  
			m2.product_category_fine = rr.product_category_fine
			and substring(m2.aggregation_value, 1, charindex(char(39), m2.aggregation_value, charindex(char(39), m2.aggregation_value)+1)) = substring(rr.aggregation_value, 1, charindex(char(39), rr.aggregation_value, charindex(char(39), rr.aggregation_value)+1)) 
			and charindex(char(39), m2.aggregation_value, charindex(char(39), m2.aggregation_value)+1) = len(m2.aggregation_value)
		left join #multiplier m3 on -- otherwise use overall level  
			m3.product_category_fine = rr.product_category_fine
			and m3.aggregation_value = 'Overall'
		left join #multiplier m4 on -- otherwise use other priced product (pick the default product for each duration)!
			substring(m4.product_category_fine, 1, CHARINDEX('month', m4.product_category_fine, 1 )+5) = substring(rr.product_category_fine, 1, CHARINDEX('month', rr.product_category_fine, 1 )+5)
			and m4.aggregation_value = 'Overall'
			and rtrim(substring(m4.product_category_fine, CHARINDEX('month', m4.product_category_fine, 1 )+6, 10 )) in ('39.95', '7.95', '19.95') -- pick the dominant price to use (if more than one have an aggregation value)  		

	where 
		rr.product_category_fine not like '%Hard Offer%' 
	) as d
where 
	net_sale_count < min_month
	and min_month_overall = 1
order by 
	product_category_fine, 
	campaign_detail, 
	net_sale_count desc

--table for ltv rollup file and to pull order level ltv  



if  object_id('tempdb..#typhoon_ltr_values') is not null 
drop table tempdb..#typhoon_ltr_values
select 
	getdate() as "Calculation Date", 
	p.product_category_fine as "Product",
	duration as "Duration",
	p.[subscription price] as "Subscription Price",
	ChannelName,
	OriginName,
	CampaignGroupName,
	Campaign,
	[Aggregation Level],
	billthrough_rate as "Billthrough Rate",
	ltr as "Non Discounted LTR",
	discounted_ltr as "Discounted LTR",
	four_year_ltr as "Non Discounted LTR after 4 years",
	discounted_four_year_ltr as "Discounted LTR after 4 years",
	ltr_pct_hist "Non Discounted LTR % Historical",
	discounted_ltr_pct_hist "Discounted LTR % Historical",
	four_year_ltr_pct_hist "Non Discounted LTR after 4 years % Historical",
	discounted_four_year_ltr_pct_hist "Discounted LTR after 4 years % Historical",
	gp.product_category as "Product Category"
into #typhoon_ltr_values
from #product_ltr p
	left join #good_products gp on gp.product_category_fine = p.product_category_fine



--create a product lookup table 
--for products new prices and no data, the product group will be switched to the 102, 752, or 751 grouping

if  object_id('tempdb..#product_lookup') is not null 
drop table tempdb..#product_lookup
select distinct
	product,
	IsTrial,
	SubscriptionDurationMonths,
	[Subscription Price],
	BusinessPartner,
	'Trial' + ' ' + cast(SubscriptionDurationMonths as varchar(2)) + ' Month ' + cast([Subscription Price] as varchar(10)) as product_category_fine,
	'Trial' + ' ' + cast(SubscriptionDurationMonths as varchar(2)) + ' Month ' as product_category
into #product_lookup
from Typhoon.dbo.Typhoon_Products p
where 
	p.IsSubscription = 1
	-- and IsTrial = 1  --allow the upgrade from free account non primary product in
	and (businesspartner = 0 or businesspartner = 11)

if  object_id('tempdb..#product_lookup2') is not null 
drop table tempdb..#product_lookup2
select 
	pl.*,
	case when ltr.[Non Discounted LTR after 4 Years] is null then 0 else 1 end as is_good_product
into #product_lookup2
from #product_lookup pl
left join #typhoon_ltr_values ltr on 
	ltr.product = pl.product_category_fine 
	and ltr.channelname = 'overall'


update #product_lookup2
set product_category_fine = null 
where is_good_product = 0

if  object_id('tempdb..#typhoon_product_lookup') is not null 
drop table tempdb..#typhoon_product_lookup
select
	pl.product,
	pl.istrial,
	pl.subscriptiondurationmonths,
	[subscription price],
	pl.businesspartner,
	coalesce(pl.product_category_fine, gp.product_category_fine) as product_category_fine
into #typhoon_product_lookup
from #product_lookup2 pl
	inner join
	(
	select *
	from
	(
		select 
			product_Category_fine, 
			product_category, 
			orders,
			max(orders) over (partition by product_category) as max_orders
		from #good_products
	) as a
	where
		orders = max_orders
	) as gp on gp.product_category = pl.product_category


-- Tables for typhoon:


drop table Typhoon.dbo.Typhoon_Ltr_Renewal_Rates
select 
	product_category_fine as [Product Category],
	aggregation_level as [Aggregation],
	aggregation_value as [Filter],
	order_month,
	net_sale_count as Period,
	cumulative_sales,
	grouped_cumulative_sales,
	month_number,
	duration,
	renewal_rate,
	rolling_renewal_rate as [Renewal Rate],
	max_sale_count, 
	[subscription price] as Price,
	campaign_detail as [Campaign Filter],
	primary_net_sales as [Primary Net Sales],
	other_net_sales as [Other Net Sales],
	multiplier as Multiplier
into Typhoon.dbo.Typhoon_Ltr_Renewal_Rates
from #typhoon_ltr_renewal_rates 


--this step is now an insert instead of recreating the table 


insert into Typhoon.dbo.Typhoon_Ltr_Values
select distinct * 
from #typhoon_ltr_values


drop table Typhoon.dbo.Typhoon_Product_Lookup
select * 
into Typhoon.dbo.Typhoon_Product_Lookup
from #typhoon_product_lookup pl

go


  





























with total_facebook_and_google as (
  select 
    ad_date,
    campaign_name,
    adset_name,
    spend,
    impressions,
    reach,
    clicks,
    leads,
    value,
    url_parameters,
    coalesce (null, 0)
  from facebook_ads_basic_daily fabd 
  left join facebook_adset fa on fa.adset_id = fabd.adset_id
  left join facebook_campaign fc on fc.campaign_id = fabd.campaign_id
  union 
  select 
    ad_date,
    campaign_name,
    adset_name,
    spend,
    impressions,
    reach,
    clicks,
    leads,
    value,
    url_parameters,
    coalesce(null, 0)
  from google_ads_basic_daily gabd 
  order by 1
  ),
  total_by_date as (
  select 
    date_trunc('month', ad_date) as ad_month,
    case
  	when url_parameters like '%utm_campaign=nan'then null
  	else substring(url_parameters, 'utm_campaign=([^&#$]+)')
    end as utm_campaign,
    sum(spend) as total_spend,
    sum(clicks) as total_clicks,
    sum(reach) as total_reach,
    sum(value) as total_value,
    round((case when sum(impressions) !=0 then sum(clicks)::numeric/sum(impressions) end),2) as ctr,
    round((case when sum(clicks) !=0 then sum(spend)::numeric/sum(clicks)end),2) as cpc,
    round(( case when sum(impressions) !=0 then sum(spend)::numeric/sum(impressions)end),2) as cpm,
    round((case when sum(spend) !=0 then (sum(value)-sum(spend))::numeric/sum(spend)end),2) as romi
 from total_facebook_and_google as tfag   
 group by ad_month, utm_campaign
),
 indicators_1m_ago as (
  select 
    lag(cpm) over(partition by utm_campaign order by ad_month ) as cpm_1m_ago,
    lag(ctr) over(partition by utm_campaign order by ad_month ) as ctr_1m_ago,
    lag(romi) over(partition by utm_campaign order by ad_month ) as romi_1m_ago
  from total_by_date as tbd
  order by 1
)
select
  tbd.ad_month,
  tbd.utm_campaign,
  sum(tbd.total_spend) as all_spend,
  sum(tbd.total_clicks) as all_clicks,
  sum(tbd.total_reach) as all_reach,
  sum(tbd.total_value) as all_value,
  sum(tbd.ctr) as all_ctr,
  sum(tbd.cpm) as all_cpm,
  sum(tbd.cpc) as all_cpc,
  sum(tbd.romi) as all_romi,
  round((sum(tbd.cpm)/sum(ima.cpm_1m_ago)-1)*100, 2) as difference_cpm,
  round((sum(tbd.ctr)/sum(ima.ctr_1m_ago)-1)*100, 2) as difference_ctr,
  round((sum(tbd.romi)/sum(ima.romi_1m_ago)-1)*100, 2) as difference_romi
from total_by_date as tbd, indicators_1m_ago as ima  
group by ad_month, utm_campaign
order by 1;




						
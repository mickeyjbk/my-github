---------------------------------------------------------------------------------------
-- Here, we pull patients who had some encounter with us between 2021 and 2022
-- These are our "Active" patients. We do this to remove patients from final query who 
-- may have received care years ago, but maybe moved to a different state, city, etc.
---------------------------------------------------------------------------------------

with active_pats as
(
	select distinct patient
	from postgres.public.encounters
	where start between '2021-01-01 00:00' and '2022-12-31 23:59'
),

---------------------------------------------------------------------------------------------
-- Exclusion 1: Transplant Cases, will use this CTE to kick them out of the ESRD cohot later 
---------------------------------------------------------------------------------------------

transplants as
(
	select *
    from postgres.public.procedures
	where code = '55.69'
),


esrd as
(
	select  cond.start as condition_start
	       ,cond.stop as condition_stop
	       ,least(trp.start,cond.stop,pat.deathdate,'2099-01-01') as esrd_end_date
	       ,cond.patient
	       ,trp.start as kidney_transplant_date
	       ,pat.first
	       ,pat.last
	       ,pat.birthdate  
	       ,pat.deathdate
	       ,pat.race
	       ,pat.state
	       ,pat.county
	       ,pat.city
	       ,FLOOR(EXTRACT(EPOCH FROM age('12-31-2022 23:59',pat.birthdate)) / 31536000) as age
	from postgres.public.conditions as cond
	join postgres.public.patients as pat
	  on cond.patient = pat.id
	join active_pats as act
	  on cond.patient = act.patient
	left join transplants as trp
	  on cond.patient = trp.patient
	where  cond.code = '585.6'
      and (  cond.stop  between '2022-01-01 00:00' and '2022-12-31 23:59'
	      or cond.start between '2022-01-01 00:00' and '2022-12-31 23:59'
		  or (cond.start < '2022-01-01 00:00' and cond.stop is null)
		  or (cond.start < '2022-01-01 00:00' and cond.stop > '2022-12-31 23:59')
		  )
	  and (pat.deathdate is null or pat.deathdate > '2022-01-01')
	  and (trp.start is null or trp.start > '2022-01-01')
),

-----------------------------------------------------------------------------------------
-- Here, we are constructing the main dates that we will need to join to the ESRD CTE
-- This will allow us to build out our patient months
-----------------------------------------------------------------------------------------

obs_dates as
(
	select cast('2022-01-01' as date) as obs_date
	union
	select cast('2022-02-01' as date) as obs_date
	union
	select cast('2022-03-01' as date) as obs_date
	union
	select cast('2022-04-01' as date) as obs_date
	union
	select cast('2022-05-01' as date) as obs_date
	union
	select cast('2022-06-01' as date) as obs_date
	union
	select cast('2022-07-01' as date) as obs_date
	union
	select cast('2022-08-01' as date) as obs_date
	union
	select cast('2022-09-01' as date) as obs_date
	union
	select cast('2022-10-01' as date) as obs_date
	union
	select cast('2022-11-01' as date) as obs_date
	union
	select cast('2022-12-01' as date) as obs_date
),



prep as
(
	select  obs.obs_date
	       ,esrd.condition_start
	       ,esrd.condition_stop
	       ,esrd.patient
	       ,esrd.kidney_transplant_date
	       ,esrd.first
	       ,esrd.last
	       ,esrd.birthdate  
	       ,esrd.deathdate
	       ,esrd.race
	       ,esrd.state
	       ,esrd.county
	       ,esrd.city
	       ,esrd.age
	from obs_dates as obs
	join esrd
	  on obs.obs_date between esrd.condition_start and esrd.esrd_end_date
),

--------------------------------------------------------------------------------------------
-- Gather all relevant labs, and determine the nth lab per each lab's patient/month combo
-- e.g. the 1st,2nd,3rd,etc. sodium pulled for Patient X, in Jan 2022
--------------------------------------------------------------------------------------------

labs as
(
	select patient
	       ,date as lab_date
	       ,date_trunc('month',date) as month
	       ,code
	       ,description
	       ,value
	       ,row_number() over (partition by patient, code, date_trunc('month',date)  order by date asc) as nth_lab
	from postgres.public.observations
    where code in ('49765-1','2947-0','1751-7','6299-2')
	  and date between '2022-01-01 00:00' and '2022-12-31 23:59'
),

final as
(    
	select distinct
            prep.obs_date
	     --  ,prep.condition_start
	     --  ,prep.condition_stop
	       ,prep.patient
	       ,prep.kidney_transplant_date
	       ,prep.first
	       ,prep.last
	       ,prep.birthdate  
	       ,prep.deathdate
	       ,prep.race
	       ,prep.state
	       ,prep.county
	       ,prep.city
	       ,prep.age
	       ,ca.value as calcium_value
	       ,na.value as sodium_value
	       ,alb.value as albumin_value
	       ,bun.value as bun_value
	from prep
	left join labs as ca
	  on prep.patient = ca.patient
	 and prep.obs_date = ca.month
	 and ca.nth_lab = 1
	 and ca.code = '49765-1' -- calcium
    left join labs as na
	  on prep.patient = na.patient
	 and prep.obs_date = na.month
	 and na.nth_lab = 1
	 and na.code = '2947-0' -- sodium
    left join labs as alb
	  on prep.patient = alb.patient
	 and prep.obs_date = alb.month
	 and alb.nth_lab = 1
	 and alb.code = '1751-7' -- albumin
    left join labs as bun
	  on prep.patient = bun.patient
	 and prep.obs_date = bun.month
	 and bun.nth_lab = 1
	 and bun.code = '6299-2' -- BUN
)

select * from final

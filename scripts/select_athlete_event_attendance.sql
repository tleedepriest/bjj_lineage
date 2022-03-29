SELECT athlete, 
	   COUNT(event_name) event_count,
       year, 
	   DENSE_RANK() OVER(ORDER BY COUNT(event_name) desc) number_events_rank
FROM bjj_lin.ibjjf_results
WHERE athlete IS NOT NULL AND Year=2022 AND gender like 'f%'
GROUP BY athlete, year
ORDER BY number_events_rank, year






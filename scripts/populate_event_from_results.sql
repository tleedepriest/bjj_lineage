INSERT INTO bjj_lin.event 
			(name, year, gi_or_no_gi)
SELECT DISTINCT event_name, year, gi_or_no_gi
FROM bjj_lin.ibjjf_results;

UPDATE bjj_lin.event
	SET
		organization = 'IBJJF', 
		rules_description = 'POINTS'
	WHERE id>=0;
	



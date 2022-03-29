INSERT INTO bjj_lin.athlete_academy 
	(athlete_id, academy_id)
	(SELECT DISTINCT 
		a.id as athlete_id,
		ac.id as academy_id
	FROM bjj_lin.ibjjf_results ir
	INNER JOIN bjj_lin.athlete a
	ON ir.athlete=a.athlete_name
	INNER JOIN bjj_lin.academy ac
	ON ir.academy=ac.academy_name
	ORDER BY a.id)



INSERT INTO bjj_lin.athlete (athlete_name)
SELECT DISTINCT UPPER(athlete) FROM bjj_lin.ibjjf_results;

INSERT INTO bjj_lin.academy (academy_name)
SELECT DISTINCT UPPER(academy) FROM bjj_lin.ibjjf_results;
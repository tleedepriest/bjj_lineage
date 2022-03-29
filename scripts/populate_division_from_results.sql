INSERT INTO bjj_lin.division (age_group, skill_rank, gender, weight_group)
SELECT DISTINCT age_group, gender, belt, weight_group FROM bjj_lin.ibjjf_results;
SELECT * FROM `NYC_TRAFFIC.NYC_TRAFFIC_VIOLATIONS` 


##1)
--Analysis of input data
--count of violation based on type --> answer:7078 'PHTO SCHOOL ZN SPEED VIOLATION'
SELECT count(violation) no_of_violation, violation as violation_type FROM `NYC_TRAFFIC.NYC_TRAFFIC_VIOLATIONS` 
group by violation_type
order by no_of_violation desc

##2)
--max count of violation based on state?
--maximum violation happened in NY state--> 7017 NY
SELECT count(violation) no_of_violation,state FROM `NYC_TRAFFIC.NYC_TRAFFIC_VIOLATIONS` 
group by state--,violation
order by no_of_violation desc

##3)
--violation_time column analysis
--most common vilation time  11:28A
---Remove any invalid violation_time
---This statement removed 7 rows from NYC_traffic_violation.
delete from `NYC_TRAFFIC.NYC_TRAFFIC_VIOLATIONS`
where violation_time not in (select violation_time from `NYC_TRAFFIC.NYC_TRAFFIC_VIOLATIONS`
where violation_time like('%A%') or violation_time  like('%P%'))

##4)
#Analysis violation_time column found that dates are invalid
most common date--12/31/2023
SELECT count(violation_time),violation_time
 FROM `NYC_TRAFFIC.NYC_TRAFFIC_VIOLATIONS`
group by violation_time
 order by 1 desc

##Data Transformations
##created table after making all transformations in the input data 
#changing dataypes of violation time to timestamp,Fineamount to numeric,converting invalid dates to valid ones 

##5)NYC_traffic_violation_dataset table
CREATE TABLE
  `NYC_TRAFFIC.NYC_TRAFFIC_VIOLATIONS_data` AS
WITH
  tranformed_data AS(
  SELECT
    a.*,
    CASE #converting violation_time into proper time format i.e AM/PM
      WHEN violation_time LIKE('%A%') THEN REPLACE(violation_time, 'A', 'AM')
      WHEN violation_time LIKE('%P%') THEN REPLACE(violation_time, 'P', 'PM')
    ELSE
    violation_time
  END
    AS violation_timestamp1,
    ##using case statment to filter invalid dates
    CAST(Fine_Amount AS NUMERIC) AS Fine_Amount_final,
    CASE
      WHEN REGEXP_CONTAINS(issue_date, r'^(?:(?:(0[13578]|1[02])/(0[1-9]|[12]\d|3[01])|(0[469]|11)/(0[1-9]|[12]\d|30)|02/(0[1-9]|1\d|2[0-8]))/(\d{4})|02/29/((\d{2}(0[48]|[2468][048]|[13579][26]))|((16|[2468][048]|[3579][26])00)))$') THEN issue_date
    ELSE
    '12/31/2023'#replacing invalid dates with most frequent date ie 12/31/2023
  END
    AS issue_date_1
  FROM
    `NYC_TRAFFIC.NYC_TRAFFIC_VIOLATIONS` a ),
  data_type_convert_date_coulmns AS (
  SELECT
    b.*,
    FORMAT_TIMESTAMP('%H:%M:%S', PARSE_TIMESTAMP('%H:%M%p', violation_timestamp1)) AS violation_timestamp_final,
    SAFE.PARSE_DATE('%m/%d/%Y', issue_date_1) AS issue_date_final
  FROM
    tranformed_data b ),
  data_enrichment AS (
  SELECT
    c.*,
    DATE_DIFF(judgment_entry_date, issue_date_final, day) AS number_of_days_for_judgement
    #calculating difference days b/w issue_date_final and judgment_entry_date
  FROM
    data_type_convert_date_coulmns c) ##removing extra unwanted columns after transformation
SELECT
  * EXCEPT(
    Fine_Amount,
    issue_date,
    issue_date_1,
    violation_time,
    violation_timestamp1,
    NTACode,
    sm)
FROM
  data_enrichment a
##joining Average_fine column to data_enrichment
LEFT JOIN
(SELECT AVG(Fine_Amount_final) Average_fine,summons_number as sm from data_enrichment
GROUP BY  violation,sm) b
ON
a.summons_number=b.sm
##joining NYC boundaries data to data_enrichment 
LEFT JOIN 
(SELECT distinct(SUBSTR(NTACode, 1, 2)) AS NTACode,BoroName as Borough
FROM 
 `NYC_TRAFFIC.NYC_boundaries`) c
ON 
a.county=c.NTACode

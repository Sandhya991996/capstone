
REVOKE APPLYBUDGET ON DATABASE PC_DBT_DB FROM ROLE PC_DBT_ROLE;
 
grant all privileges on DATABASE PC_DBT_DB to role PC_DBT_ROLE;
 
grant all privileges on schema public to role PC_DBT_ROLE;
 
grant select on all tables in schema public to role PC_DBT_ROLE;

 GRANT SELECT ON FUTURE TABLES IN DATABASE PC_DBT_DB TO ROLE PC_DBT_ROLE;

create or replace function trans_func(amount number)
returns string
language sql 
as
$$
select case when amount > 500 then 'High'
        when amount between 100 and 200 then 'Medium'
        else 'Low' end
 
$$
;

grant USAGE on FUNCTION trans_func(NUMBER) to role PC_DBT_ROLE;
 
select * from transaction ;
select *, trans_func(amount) as risk_level from transaction; 

---------------------------------------------------------------------------------------------------

create or replace function age_func(age number)
returns string
language sql 
as
$$
select case when age > 100 then 'invalid'
        when age < 0 then 'invalid'
        else 'valid' end
 
$$
;

--age = today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
    
    --return age

-------------------------------------------------------o

CREATE OR REPLACE FUNCTION calculate_age(dob DATE)
RETURNS INT
LANGUAGE SQL
AS
$$
    -- Calculate the age
    SELECT DATEDIFF(
        YEAR,
        dob,
        CURRENT_DATE
    ) - CASE
        WHEN MONTH(CURRENT_DATE) < MONTH(dob) OR (MONTH(CURRENT_DATE) = MONTH(dob) AND DAY(CURRENT_DATE) < DAY(dob))
        THEN 'invalid'
        ELSE 'valid'
    END
$$;


SELECT *,calculate_age(date_of_birth) AS age from customer;

    
grant USAGE on FUNCTION age_func(NUMBER) to role PC_DBT_ROLE;
 
select * from customer limit 10 ;
select *, age_func(date_of_birth) as age_status  from customer; 


-------------------------------------------------------
CREATE OR REPLACE FUNCTION calculate_age_status(dob DATE)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
  age INT;
  today DATE;
BEGIN
  today := CURRENT_DATE;
  age := DATE_PART('YEAR', today) - DATE_PART('YEAR', dob);
  
  -- Adjust age if the birthday hasn't occurred yet this year
  IF (DATE_PART('DOY', today) < DATE_PART('DOY', dob)) THEN
    age := age - 1;
  END IF;
  
  -- Return validity status based on age
  IF age > 100 THEN
    RETURN 'Invalid';
  ELSIF age < 0 THEN
    RETURN 'Invalid';
  ELSE
    RETURN 'Valid';
  END IF;
END;
$$;

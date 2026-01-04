---- Create data source ----
CREATE TABLE IF NOT EXISTS "data_source_LEADS" (
  "id" INTEGER,
  "date_of_last_request" TEXT,
  "buyer" INTEGER,
  "seller" INTEGER,
  "best_time_to_call" TEXT,
  "budget" REAL,
  "created_at" TEXT,
  "updated_at" TEXT,
  "user_id" REAL,
  "location" TEXT,
  "date_of_last_contact" TEXT,
  "status_name" TEXT,
  "commercial" INTEGER,
  "merged" REAL,
  "area_id" REAL,
  "compound_id" REAL,
  "developer_id" REAL,
  "meeting_flag" REAL,
  "do_not_call" INTEGER,
  "lead_type_id" INTEGER,
  "customer_id" INTEGER,
  "method_of_contact" TEXT,
  "lead_source" TEXT,
  "campaign" TEXT,
  "lead_type" TEXT
);

CREATE TABLE IF NOT EXISTS "data_source_SALES" (
  "id" INTEGER,
  "lead_id" INTEGER,
  "unit_value" REAL,
  "unit_location" TEXT,
  "expected_value" REAL,
  "actual_value" REAL,
  "date_of_reservation" TEXT,
  "reservation_update_date" TEXT,
  "date_of_contraction" TEXT,
  "property_type_id" INTEGER,
  "area_id" REAL,
  "compound_id" REAL,
  "sale_category" TEXT,
  "years_of_payment" REAL,
  "property_type" TEXT
);

---- Create dwh ----

CREATE TABLE IF NOT EXISTS "dwh_LEADS" (
  "id" INTEGER NOT NULL PRIMARY KEY,
  "src_id" INTEGER NOT NULL,
  "date_of_last_request" TEXT NOT NULL,
  "buyer" INTEGER NOT NULL,
  "seller" INTEGER NOT NULL,
  "best_time_to_call" TEXT,
  "budget" REAL,
  "created_at" TEXT NOT NULL,
  "updated_at" TEXT NOT NULL,
  "user_id" REAL,
  "location" TEXT,
  "date_of_last_contact" TEXT,
  "status_name" TEXT NOT NULL,
  "commercial" INTEGER NOT NULL,
  "merged" REAL,
  "area_id" REAL,
  "compound_id" REAL,
  "developer_id" REAL,
  "meeting_flag" REAL,
  "do_not_call" INTEGER NOT NULL,
  "lead_type_id" INTEGER NOT NULL,
  "customer_id" INTEGER NOT NULL,
  "method_of_contact" TEXT NOT NULL,
  "lead_source" TEXT,
  "campaign" TEXT,
  "lead_type" TEXT NOT NULL,
  "effective_start_date" TEXT NOT NULL,
  "effective_end_date" TEXT,
  "is_current" INTEGER NOT NULL,
  "version_id" INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS "dwh_SALES" (
  "id" INTEGER NOT NULL PRIMARY KEY,
  "src_id" INTEGER  NOT NULL,
  "lead_id" INTEGER NOT NULL,
  "unit_value" REAL,
  "unit_location" TEXT,
  "expected_value" REAL,
  "actual_value" REAL,
  "date_of_reservation" TEXT,
  "reservation_update_date" TEXT,
  "date_of_contraction" TEXT,
  "property_type_id" INTEGER NOT NULL,
  "area_id" REAL,
  "compound_id" REAL,
  "sale_category" TEXT,
  "years_of_payment" REAL,
  "property_type" TEXT NOT NULL,
  "effective_start_date" TEXT NOT NULL,
  "effective_end_date" TEXT,
  "is_current" INTEGER NOT NULL,
  "version_id" INTEGER NOT NULL
);

---- Create stars ----

DROP VIEW IF EXISTS star_LEADS;
CREATE VIEW star_LEADS AS
	SELECT 
		src_id as id,
		date_of_last_request,
		buyer,seller,
		best_time_to_call,
		budget,
		created_at,
		updated_at,
		user_id,
		location,
		date_of_last_contact,
		status_name,
		commercial,
		merged,
		area_id,
		compound_id,
		developer_id,
		meeting_flag,
		do_not_call,
		lead_type_id,
		customer_id,
		method_of_contact,
		lead_source,
		campaign,
		lead_type
	FROM 
		dwh_LEADS
	WHERE 
		is_current=True;
		
DROP VIEW IF EXISTS star_SALES;
CREATE VIEW star_SALES AS	
	SELECT 
	  src_id as id,
	  lead_id,
	  unit_value,
	  unit_location,
	  expected_value,
	  actual_value,
	  date_of_reservation,
	  reservation_update_date,
	  date_of_contraction,
	  property_type_id,
	  area_id,
	  compound_id,
	  sale_category,
	  years_of_payment,
	  property_type
	FROM 
		dwh_SALES
	WHERE 
		is_current=True;


DROP VIEW IF EXISTS star_lead_to_sale_summary;
CREATE VIEW star_lead_to_sale_summary AS
SELECT 
	l.location as "Location",
	l.status_name as "Status Name" , 
	l.method_of_contact as "Method of contact", 
	l.campaign as "Campaign",
	CAST(
		julianday(
		  CASE
			WHEN l.updated_at IS NULL THEN (substr(l.created_at,7,4)||'-'||substr(l.created_at,1,2)||'-'||substr(l.created_at,4,2)||' '||substr(l.created_at,12))
			WHEN (substr(l.updated_at,7,4)||'-'||substr(l.updated_at,1,2)||'-'||substr(l.updated_at,4,2)||' '||substr(l.updated_at,12))
			   < (substr(l.created_at,7,4)||'-'||substr(l.created_at,1,2)||'-'||substr(l.created_at,4,2)||' '||substr(l.created_at,12))
			THEN (substr(l.created_at,7,4)||'-'||substr(l.created_at,1,2)||'-'||substr(l.created_at,4,2)||' '||substr(l.created_at,12))
			ELSE (substr(l.updated_at,7,4)||'-'||substr(l.updated_at,1,2)||'-'||substr(l.updated_at,4,2)||' '||substr(l.updated_at,12))
		  END
		)
		- julianday(substr(l.created_at,7,4)||'-'||substr(l.created_at,1,2)||'-'||substr(l.created_at,4,2)||' '||substr(l.created_at,12))
	AS INTEGER) as "Time from created", 
	COUNT(s.lead_id) as "Sales count",
	AVG(s.unit_value) as "Avg Unit Value",
	AVG(s.actual_value) as "Avg Actual Value",
	AVG(s.expected_value) as "Avg Expected Value", 
	AVG(s.actual_value - expected_value) as "Avg Variance Value", 
	json_group_array(DISTINCT property_type) as "Property Types"
FROM 
	star_LEADS L
	LEFT JOIN star_SALES s ON l.id = s.lead_id
GROUP BY l.id;
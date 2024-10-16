ALTER TABLE orders_table
	ALTER COLUMN date_uuid TYPE UUID
		USING date_uuid::UUID,
	ALTER COLUMN user_uuid TYPE UUID
		USING user_uuid::UUID,
	ALTER COLUMN card_number TYPE VARCHAR(19),
	ALTER COLUMN product_code TYPE VARCHAR(11),
	ALTER COLUMN product_quantity TYPE SMALLINT;

ALTER TABLE dim_users
	DROP COLUMN IF EXISTS index,
	ALTER COLUMN user_uuid TYPE uuid
		USING user_uuid::uuid,
	ADD CONSTRAINT user_uuid_unq UNIQUE(user_uuid),
	ADD PRIMARY KEY (user_uuid),
	ALTER COLUMN first_name TYPE VARCHAR(255),
	ALTER COLUMN last_name TYPE VARCHAR(255),
	ALTER COLUMN date_of_birth TYPE DATE,
	ALTER COLUMN country_code TYPE VARCHAR(3),
	ALTER COLUMN join_date TYPE DATE;

UPDATE dim_store_details
	SET longitude = NULL,
		latitude = NULL,
		address = NULL,
		locality = NULL
		WHERE store_type = 'Web Portal';

UPDATE dim_store_details
SET staff_numbers = regexp_replace(dim_store_details.staff_numbers, '[\D+]', '');

ALTER TABLE dim_store_details
	ALTER COLUMN longitude TYPE NUMERIC
		USING CASE 
				WHEN longitude IS NULL THEN NULL
				ELSE longitude::numeric
				END,
	ADD PRIMARY KEY (store_code),
	ADD CONSTRAINT store_code_unq UNIQUE(store_code),
	ALTER COLUMN locality TYPE VARCHAR(255),
	ALTER COLUMN store_code TYPE VARCHAR(25),
	ALTER COLUMN staff_numbers TYPE SMALLINT
		USING staff_numbers::numeric,
	ALTER COLUMN opening_date TYPE DATE,
	ALTER COLUMN store_type TYPE VARCHAR(255),
	ALTER COLUMN latitude TYPE NUMERIC
		USING CASE 
				WHEN latitude IS NULL THEN NULL
				ELSE latitude::numeric
				END,
	ALTER COLUMN country_code TYPE VARCHAR(3),
	ALTER COLUMN continent TYPE VARCHAR(9);

UPDATE dim_products
SET product_price = regexp_replace(dim_products.product_price, '[£]', '');

UPDATE dim_products
SET weight = regexp_replace(dim_products.weight, '[\D]]', '0');

UPDATE dim_products
SET weight = regexp_replace(dim_products.weight, '', '0');

UPDATE dim_products
SET weight = regexp_replace(dim_products.weight, '[\D+]', '0');


ALTER TABLE dim_products
ALTER COLUMN weight TYPE NUMERIC
	USING weight::numeric;

ALTER TABLE dim_products
ADD COLUMN IF NOT EXISTS weight_class VARCHAR(14);

UPDATE dim_products
SET weight_class = 'Light'
WHERE weight < 2;

UPDATE dim_products
SET weight_class = 'Mid_Sized'
WHERE weight >= 2 AND weight < 40;

UPDATE dim_products
SET weight_class = 'Heavy'
WHERE weight >= 40 AND weight < 140;

UPDATE dim_products
SET weight_class = 'Truck_Required'
WHERE weight >= 140;

UPDATE dim_products
SET removed = regexp_replace(dim_products.removed, 'Still_avaliable', 'true');

UPDATE dim_products
SET removed = regexp_replace(dim_products.removed, 'remove', 'false');

UPDATE dim_products
SET removed = regexp_replace(dim_products.removed, 'Removed', 'false');

ALTER TABLE dim_products
	ALTER COLUMN product_price TYPE NUMERIC
		USING product_price::numeric,
	ADD PRIMARY KEY (product_code),
	ADD CONSTRAINT product_code_unq UNIQUE(product_code),
	ALTER COLUMN "EAN" TYPE VARCHAR(20),
	ALTER COLUMN product_code TYPE VARCHAR(11),
	ALTER COLUMN date_added TYPE DATE,
	ALTER COLUMN uuid TYPE uuid
		USING uuid::uuid,
	ALTER COLUMN removed TYPE BOOLEAN
		USING removed::BOOLEAN;

ALTER TABLE dim_date_times
ADD COLUMN "year" INT,
ADD COLUMN "month" INT,
ADD COLUMN "day" INT;

UPDATE dim_date_times
SET "year" = EXTRACT(YEAR FROM time),
    "month" = EXTRACT(MONTH FROM time),
    "day" = EXTRACT(DAY FROM time);


ALTER TABLE dim_date_times
	ALTER COLUMN "month" TYPE VARCHAR(2),
	ALTER COLUMN "year" TYPE VARCHAR(4),
	ALTER COLUMN "day" TYPE VARCHAR(2),
	ALTER COLUMN timestamp TYPE TIME
		USING timestamp::TIME,
	ALTER COLUMN time_period TYPE VARCHAR(10),
	ALTER COLUMN date_uuid TYPE uuid
		USING date_uuid::uuid,
	ADD PRIMARY KEY (date_uuid),
	ADD CONSTRAINT date_uuid_unq UNIQUE (date_uuid);

ALTER TABLE dim_card_details
	ALTER COLUMN card_number TYPE VARCHAR(19),
	ALTER COLUMN expiry_date TYPE VARCHAR(19),
	ALTER COLUMN date_payment_confirmed TYPE DATE,
	ADD PRIMARY KEY (card_number),
	ADD CONSTRAINT card_number_unq UNIQUE(card_number);

ALTER TABLE orders_table
	ADD CONSTRAINT fk_dim_users
		FOREIGN KEY (user_uuid)
			REFERENCES dim_users(user_uuid),
	ADD CONSTRAINT fk_dim_date_times
		FOREIGN KEY (date_uuid)
			REFERENCES dim_date_times(date_uuid),
	ADD CONSTRAINT fk_dim_products
		FOREIGN KEY (product_code)
			REFERENCES dim_products(product_code),
	ADD CONSTRAINT fk_dim_store_details
		FOREIGN KEY (store_code)
			REFERENCES dim_store_details(store_code);
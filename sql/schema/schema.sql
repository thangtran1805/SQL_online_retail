CREATE DATABASE online_retail;

\c online_retail


CREATE TABLE dim_categories (
	category_id BIGINT PRIMARY KEY,
	category_code TEXT,
	category_update_time_stamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	
);

CREATE TABLE dim_users (
	user_id BIGINT PRIMARY KEY,
	user_update_time_stamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE dim_products (
	product_id BIGINT PRIMARY KEY,
	category_id BIGINT,
	brand TEXT,
	price NUMERIC,
	product_update_time_stamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	CONSTRAINT fk_products_category 
		FOREIGN KEY (category_id)
		REFERENCES dim_categories(category_id),
	CONSTRAINT check_products_price
		CHECK (price >= 0)
);

CREATE TABLE fact_sessions (
	user_session TEXT PRIMARY KEY,
	session_user_id BIGINT REFERENCES dim_users(user_id),
	session_user_update_time_stamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	CONSTRAINT fk_sessions_user
		FOREIGN KEY (session_user_id)
		REFERENCES dim_users(user_id)
);

CREATE TABLE fact_events(
	event_id SERIAL PRIMARY KEY,
	event_time TIMESTAMP,
	event_type TEXT CHECK (event_type IN ('view','cart','purchase')),
	event_user_id BIGINT REFERENCES dim_users(user_id),
	event_product_id BIGINT REFERENCES dim_products(product_id),
	event_user_session TEXT REFERENCES fact_sessions(user_session),
	event_update_time_stamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

	-- Constraints
	CONSTRAINT fk_events_user
		FOREIGN KEY (event_user_id)
		REFERENCES dim_users(user_id),
	CONSTRAINT fk_events_product
		FOREIGN KEY (event_product_id)
		REFERENCES dim_products(product_id),
	CONSTRAINT fk_events_session
		FOREIGN KEY (event_user_session)
		REFERENCES fact_sessions(user_session),
	CONSTRAINT check_event_type
		CHECK (event_type IN ('view','cart','purchase'))
);

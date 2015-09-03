CREATE TABLE IF NOT EXISTS all_types (
	x_ascii ascii,
	x_bigint bigint,
	x_blob blob,
	x_boolean boolean,
	x_decimal decimal,
	x_double double,
	x_float float,
	x_inet inet,
	x_int int,
	x_text text,
	x_timestamp timestamp,
	x_timeuuid timeuuid,
	x_uuid uuid,
	x_varchar varchar,
	x_varint varint,
	rowkey int,
	cellkey timestamp,
	PRIMARY KEY (rowkey, cellkey)
);

CREATE TABLE IF NOT EXISTS orders (
	customer_id uuid,
	date timestamp,
	description text,
	items int,
	total decimal,
	weight double,
	shipped boolean,
	PRIMARY KEY (customer_id, date)
);

CREATE TABLE IF NOT EXISTS shipments (
	customer_id uuid,
	date timestamp,
	items int,
	total decimal,
	weight double,
	PRIMARY KEY (customer_id, date)
);

CREATE INDEX order_description ON orders(description);

INSERT INTO all_types(rowkey, cellkey, x_ascii, x_bigint, x_blob, x_boolean, x_decimal, x_double, x_float)
	VALUES(1, '2015-09-01', 'blah', 12345, textAsBlob('blah'), true, 123.45, 123.45, 123.45); 
INSERT INTO all_types(rowkey, cellkey, x_ascii, x_bigint, x_blob, x_boolean, x_decimal, x_double, x_float)
	VALUES(1, '2015-09-02', 'blah', 12345, textAsBlob('blah'), true, 123, 123, 123); 
INSERT INTO all_types(rowkey, cellkey, x_inet, x_int, x_text, x_timestamp, x_timeuuid, x_uuid, x_varchar, x_varint)
	VALUES(2, '2015-09-03', '127.0.0.1', 123, 'blah', '2015-09-02 15:30', fa1ebd08-51d4-11e5-885d-feff819cdc9f, 
	de305d54-75b4-431b-adb2-eb6b9e546014, 'blah', 12345);

INSERT INTO orders(customer_id, date, description, items, total, weight, shipped) VALUES(c7b44500-b6bf-11e4-a71e-12e3f512a338, '2015-01-02', 'clothes', 3, 215.00, 9.5, false);
INSERT INTO orders(customer_id, date, description, items, total, weight, shipped) VALUES(c7b44500-b6bf-11e4-a71e-12e3f512a338, '2015-01-10', 'groceries', 10, 155.99, 15.0, true);
INSERT INTO orders(customer_id, date, description, items, total, weight, shipped) VALUES(c7b44500-b6bf-11e4-a71e-12e3f512a338, '2015-02-01', 'clothes', 4, 410.05, 4, false);

INSERT INTO orders(customer_id, date, description, items, total, weight, shipped) VALUES(c7b44cb2-b6bf-11e4-a71e-12e3f512a338, '2015-01-05', 'furniture', 1, 620.00, 650.0, true);

INSERT INTO orders(customer_id, date, description, items, total, weight, shipped) VALUES(d3fdccdc-b6bf-11e4-a71e-12e3f512a338, '2015-01-01', 'toys', 2, 43.15, 2, false);
INSERT INTO orders(customer_id, date, description, items, total, weight, shipped) VALUES(d3fdccdc-b6bf-11e4-a71e-12e3f512a338, '2015-02-10', 'electronics', 2, 730.56, 45.5, true);

INSERT INTO orders(customer_id, date, description, items, total, weight, shipped) VALUES(d3fdcf34-b6bf-11e4-a71e-12e3f512a338, '2015-02-15', 'jewelry', 1, 1150.00, 1, false);
INSERT INTO orders(customer_id, date, description, items, total, weight, shipped) VALUES(d3fdcf34-b6bf-11e4-a71e-12e3f512a338, '2015-01-25', 'electronics', 3, 290.14, 74.0, false);

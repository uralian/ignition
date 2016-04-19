-- drop old tables

DROP TABLE IF EXISTS shipments;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS customers;

-- create tables

CREATE TABLE customers (
	customer_id UUID,
	name VARCHAR,
	address VARCHAR,
	PRIMARY KEY (customer_id)
);

CREATE TABLE orders (
	order_id UUID,
	customer_id UUID REFERENCES customers(customer_id),
	date DATE,
	description VARCHAR,
	items INT,
	total DECIMAL,
	weight DOUBLE,
	shipped BOOLEAN,
	PRIMARY KEY (order_id)
);

CREATE TABLE shipments (
	order_id UUID REFERENCES orders(order_id),
	shipment_no INT,
	date DATE,
	items INT,
	total DECIMAL,
	weight DOUBLE,
	PRIMARY KEY (order_id, shipment_no)
);

-- insert customers

INSERT INTO customers(customer_id, name, address) VALUES('c7b44500-b6bf-11e4-a71e-12e3f512a338', 'Apple', 'Cupertino, CA');
INSERT INTO customers(customer_id, name, address) VALUES('de305d54-75b4-431b-adb2-eb6b9e546014', 'Walmart', null);

-- insert orders

INSERT INTO orders(order_id, customer_id, date, description, items, total, weight, shipped) VALUES('f3b3d5bc-ea26-11e5-9ce9-5e5517507c66', 'c7b44500-b6bf-11e4-a71e-12e3f512a338', '2015-01-01', 'toys', 2, 43.15, 2, false);
INSERT INTO orders(order_id, customer_id, date, description, items, total, weight, shipped) VALUES('f3b3d990-ea26-11e5-9ce9-5e5517507c66', 'c7b44500-b6bf-11e4-a71e-12e3f512a338', '2015-02-10', 'electronics', 2, 730.56, 45.5, true);
INSERT INTO orders(order_id, customer_id, date, description, items, total, weight, shipped) VALUES('f3b3dcd8-ea26-11e5-9ce9-5e5517507c66', 'c7b44500-b6bf-11e4-a71e-12e3f512a338', '2015-01-25', 'electronics', 3, 290.14, 74.0, false);

INSERT INTO orders(order_id, customer_id, date, description, items, total, weight, shipped) VALUES('f3b3df9e-ea26-11e5-9ce9-5e5517507c66', 'de305d54-75b4-431b-adb2-eb6b9e546014', '2015-01-02', 'clothes', 3, 215.00, 9.5, false);
INSERT INTO orders(order_id, customer_id, date, description, items, total, weight, shipped) VALUES('f3b3e2e6-ea26-11e5-9ce9-5e5517507c66', 'de305d54-75b4-431b-adb2-eb6b9e546014', '2015-01-10', 'groceries', 10, 155.99, 15.0, true);
INSERT INTO orders(order_id, customer_id, date, description, items, total, weight, shipped) VALUES('f3b3e5a2-ea26-11e5-9ce9-5e5517507c66', 'de305d54-75b4-431b-adb2-eb6b9e546014', '2015-02-01', 'clothes', 4, 410.05, 4, false);
INSERT INTO orders(order_id, customer_id, date, description, items, total, weight, shipped) VALUES('f3b3e8cc-ea26-11e5-9ce9-5e5517507c66', 'de305d54-75b4-431b-adb2-eb6b9e546014', '2015-01-05', 'furniture', 1, 620.00, 650.0, true);
INSERT INTO orders(order_id, customer_id, date, description, items, total, weight, shipped) VALUES('f3b3eb4c-ea26-11e5-9ce9-5e5517507c66', 'de305d54-75b4-431b-adb2-eb6b9e546014', '2015-02-15', 'jewelry', 1, 1150.00, 1, false);

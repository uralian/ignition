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

CREATE INDEX order_description ON orders(description);

INSERT INTO orders(customer_id, date, description, items, total, weight, shipped) VALUES(c7b44500-b6bf-11e4-a71e-12e3f512a338, '2015-01-02', 'clothes', 3, 215.00, 9.5, false);
INSERT INTO orders(customer_id, date, description, items, total, weight, shipped) VALUES(c7b44500-b6bf-11e4-a71e-12e3f512a338, '2015-01-10', 'groceries', 10, 155.99, 15.0, true);
INSERT INTO orders(customer_id, date, description, items, total, weight, shipped) VALUES(c7b44500-b6bf-11e4-a71e-12e3f512a338, '2015-02-01', 'clothes', 4, 410.05, 4, false);

INSERT INTO orders(customer_id, date, description, items, total, weight, shipped) VALUES(c7b44cb2-b6bf-11e4-a71e-12e3f512a338, '2015-01-05', 'furniture', 1, 620.00, 650.0, true);

INSERT INTO orders(customer_id, date, description, items, total, weight, shipped) VALUES(d3fdccdc-b6bf-11e4-a71e-12e3f512a338, '2015-01-01', 'toys', 2, 43.15, 2, false);
INSERT INTO orders(customer_id, date, description, items, total, weight, shipped) VALUES(d3fdccdc-b6bf-11e4-a71e-12e3f512a338, '2015-02-10', 'electronics', 2, 730.56, 45.5, true);

INSERT INTO orders(customer_id, date, description, items, total, weight, shipped) VALUES(d3fdcf34-b6bf-11e4-a71e-12e3f512a338, '2015-02-15', 'jewelry', 1, 1150.00, 1, false);
INSERT INTO orders(customer_id, date, description, items, total, weight, shipped) VALUES(d3fdcf34-b6bf-11e4-a71e-12e3f512a338, '2015-01-25', 'electronics', 3, 290.14, 74.0, false);

-- Optional Azure SQL source setup. The default portfolio run uses the supplied
-- CSV extract so the pipeline can be reproduced without Azure SQL credentials.
CREATE TABLE dbo.Refunds (
    refund_id INT NOT NULL PRIMARY KEY,
    payment_id INT NOT NULL,
    refund_timestamp DATETIME2 NOT NULL,
    refund_amount DECIMAL(10, 2) NOT NULL,
    refund_reason NVARCHAR(255) NOT NULL
);

INSERT INTO dbo.Refunds (refund_id, payment_id, refund_timestamp, refund_amount, refund_reason)
VALUES
(7001, 5002, '2024-10-20 11:30:00', 5.00, 'Order Cancelled:Customer'),
(7002, 5003, '2024-11-10 14:45:00', 10.00, 'Product Returned:Customer');

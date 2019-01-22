#Problem 1
SELECT DISTINCT
    vendor_name
FROM
    vendors
WHERE
    vendor_name IN (SELECT 
            vendor_name
        FROM
            invoices
        WHERE
            invoices.vendor_id = vendors.vendor_id)
ORDER BY vendor_name;
#Problem 2
SELECT 
    invoice_number AS 'Invoice #',
    invoice_total AS 'Invoice Total'
FROM
    invoices
WHERE
    payment_total > (SELECT 
            AVG(payment_total)
        FROM
            invoices
        WHERE
            payment_total > 0)
ORDER BY invoice_total DESC;
#Problem 3
SELECT 
    account_number AS 'Account Number',
    account_description AS 'Account Description'
FROM
    general_ledger_accounts la
WHERE
    NOT EXISTS( SELECT 
            account_number
        FROM
            invoice_line_items
        WHERE
            account_number = la.account_number)
ORDER BY account_number;
#Problem 4
SELECT 
    vendor_name,
    i.invoice_id,
    invoice_sequence,
    line_item_amount
FROM
    vendors v
        JOIN
    invoices i ON v.vendor_id = i.vendor_id
        JOIN
    invoice_line_items li ON i.invoice_id = li.invoice_id
WHERE
    i.invoice_id IN (SELECT DISTINCT
            invoice_id
        FROM
            invoice_line_items
        WHERE
            invoice_sequence > 1)
GROUP BY vendor_name , i.invoice_id , invoice_sequence;
#Problem 5
SELECT 
    SUM(balance_due) AS 'Sum of largest unpaid invoices'
FROM
    (SELECT 
        vendor_id,
            MAX(invoice_total) AS balance_due
    FROM
        invoices
    WHERE
        invoice_total - payment_total - credit_total > 0
    GROUP BY vendor_id) t;
#Problem 6
SELECT 
    ANY_VALUE(v.vendor_name) AS 'Vendor Name',
    v.vendor_city AS 'Vendor City',
    v.vendor_state AS 'Vendor State'
FROM
    vendors v
        LEFT OUTER JOIN
    (SELECT 
        vendor_name, CONCAT(vendor_city, vendor_state) AS city_state
    FROM
        vendors) t ON v.vendor_name = t.vendor_name
GROUP BY v.vendor_state , v.vendor_city
HAVING COUNT(*) = 1;
#Problem 7
SELECT 
    vendor_name, invoice_number, invoice_date, invoice_total
FROM
    invoices i
        JOIN
    vendors v ON i.vendor_id = v.vendor_id
WHERE
    invoice_date IN (SELECT 
            MIN(invoice_date)
        FROM
            invoices
        WHERE
            vendor_id = i.vendor_id
        GROUP BY vendor_id)
ORDER BY vendor_name
#Problem 8
SELECT 
    vendor_name AS 'Vendor Name',
    invoice_number AS 'Invoice #',
    invoice_date AS 'Invoice date',
    invoice_total AS 'Invoice Total'
FROM
    invoices i
        JOIN
    (SELECT 
        vendor_id, MIN(invoice_date) AS oldest
    FROM
        invoices
    GROUP BY vendor_id) oi ON i.vendor_id = oi.vendor_id
        AND i.invoice_date = oi.oldest
        JOIN
    vendors v ON i.vendor_id = v.vendor_id
ORDER BY vendor_name
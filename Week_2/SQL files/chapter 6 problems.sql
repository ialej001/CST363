#Problem 1
SELECT 
    v.vendor_id, SUM(i.invoice_total) AS invoice_total
FROM
    invoices AS i
        JOIN
    vendors AS v ON i.vendor_id = v.vendor_id
GROUP BY vendor_id
ORDER BY invoice_total DESC;
#Problem 2
SELECT 
    v.vendor_name, SUM(i.payment_total) AS payment_total
FROM
    invoices AS i
        JOIN
    vendors AS v ON i.vendor_id = v.vendor_id
GROUP BY vendor_name
ORDER BY payment_total DESC;
#Problem 3
SELECT 
    v.vendor_name,
    COUNT(*) AS number_of_invoices,
    SUM(i.invoice_total) AS invoice_total
FROM
    invoices AS i
        JOIN
    vendors AS v ON i.vendor_id = v.vendor_id
GROUP BY vendor_name
ORDER BY COUNT(*) DESC;
#Problem 4
SELECT 
    la.account_description AS 'Account Description',
    COUNT(*) AS 'Number of Line Items',
    SUM(li.line_item_amount) AS 'Line Item Total'
FROM
    invoice_line_items AS li
        JOIN
    general_ledger_accounts AS la ON li.account_number = la.account_number
GROUP BY li.account_number
HAVING COUNT(*) > 1;
#Problem 5
SELECT 
    la.account_description AS 'Account Description',
    COUNT(*) AS 'Number of Line Items',
    SUM(li.line_item_amount) AS 'Line Item Total'
FROM
    invoice_line_items li
        JOIN
    general_ledger_accounts la ON li.account_number = la.account_number
        JOIN
    invoices i ON li.invoice_id = i.invoice_id
WHERE
    i.invoice_date BETWEEN '2014-04-01' AND '2014-06-30'
GROUP BY li.account_number
HAVING COUNT(*) > 1;
#Problem 6
SELECT 
    li.account_number AS 'Account Number',
    SUM(li.line_item_amount) AS 'Line Item Amount'
FROM
    general_ledger_accounts la
        JOIN
    invoice_line_items li ON la.account_number = li.account_number
GROUP BY li.account_number WITH ROLLUP;
#Problem 7
SELECT 
    v.vendor_name AS 'Vendor Name',
    COUNT(DISTINCT li.account_number) AS 'Distinct Accounts'
FROM
    vendors v
        JOIN
    invoices i ON v.vendor_id = i.vendor_id
        JOIN
    invoice_line_items li ON i.invoice_id = li.invoice_id
GROUP BY vendor_name
HAVING COUNT(DISTINCT li.account_number) > 1
ORDER BY v.vendor_name;
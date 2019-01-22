#Problem 1
INSERT INTO ap.terms VALUES
(6, 'Net due 120 days', 120);
#Problem 2
UPDATE ap.terms 
SET 
    terms_description = 'Net due 125 days',
    terms_due_days = 125
WHERE
    terms_id = 6;
#Problem 3
DELETE FROM ap.terms 
WHERE
    terms_id = 6;
#Problem 4
INSERT INTO ap.invoices VALUES
(DEFAULT, 32, 'AX-014-027', '2014-8-1', 434.58, 0, 0, 2, '2014-8-31', NULL);
#Problem 5
INSERT INTO ap.invoice_line_items VALUES
(115, 1, 160, 180.23, 'Hard Drive'),
(115, 2, 527, 254.35, 'Exchange Server update');
#Problem 6
UPDATE ap.invoices 
SET 
    credit_total = 434.58 * .1,
    payment_total = 434.58 * .9
WHERE
    invoice_id = 115;
#Problem 7
UPDATE ap.vendors 
SET 
    default_account_number = 403
WHERE
    vendor_id = 44;
#Problem 8
UPDATE ap.invoices 
SET 
    terms_id = 2
WHERE
    vendor_id = (SELECT 
            vendor_id
        FROM
            ap.vendors
        WHERE
            default_account_number = 2);
#Problem 9
DELETE FROM ap.invoice_line_items 
WHERE
    invoice_id = 115;
    
DELETE FROM ap.invoices 
WHERE
    invoice_id = 115;
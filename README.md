# Customer Analysis with spark, gradle, java
The objective of this is to get insights from the data. Datasets are attached in the "hackatron" directory. The schema of the data is as follows:

Product: (product_id, product_name, product_type, product_version, product_price)
Customer :(customer_id, customer_first_name, customer_last_name, phone_number)
Sales : (transaction_id, customer_id, product_id, timestamp,total_amount, total_quantity)
Refund : (refund_id,original_transaction_id, customer_id, product_id, timestamp, refund_amount, refund)

Insights:
1. Distribution of sales by product name and product type.
2. Calculate the total amount of all transactions that happened in year 2013 and have not been refunded as of today.
3. Display the customer name who made the second most purchases in the month of May 2013. Refunds should be excluded.
4. Find a product that has not been sold at least once(if any).
5. Calculate the total number of users who purchased the same product consecutively at least 2 times on a day.



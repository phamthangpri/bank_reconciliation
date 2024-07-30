__Bank Statement Reconciliation Project__
This project aims to reconcile bank statements with back office data. There are three types of transactions in bank statements: transfer, check, and direct debit.

Technical Issues
- Bank Statement Data:
+ The data is sourced from MT940 structured files. To extract the client's name in bank transfers, we use REGEX to parse the transaction motif into smaller parts.
+ Clients may make multiple bank transactions to pay for a product.

- Challenges:
+ The total amount sometimes does not match the amount due.
+ The sender's name may not be the client's name (it could be a family member such as a spouse, parent, etc.).
+ There can be up to a 60-day difference between the bank transaction date and the back office transaction date.

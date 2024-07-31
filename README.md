# Bank Statement Reconciliation Project

This project aims to reconcile bank statements with back office data. There are three types of transactions in bank statements: transfer, check, and direct debit.

## Technical Issues

### Bank Statement Data
- The data is sourced from MT940 structured files. To extract the client's name in bank transfers, we use REGEX to parse the transaction motif into smaller parts.
- Clients may make multiple bank transactions to pay for a product.

### Challenges
- The total amount sometimes does not match the amount due.
- The sender's name may not be the client's name (it could be a family member such as a spouse, parent, etc.).
- There can be up to a 60-day difference between the bank transaction date and the back office transaction date.

## Features
- **Automated Data Parsing**: Extract and parse data from MT940 files using REGEX.
- **Transaction Matching**: Match transactions from bank statements with back office records.
- **Discrepancy Detection**: Identify and highlight discrepancies between bank statement transactions and back office records.
- **Report Generation**: Generate detailed reports of reconciled and unreconciled transactions.

## Prerequisites
- Python 3.x
- Required libraries: `pandas`, `numpy`, `re`, `openpyxl`, `xlrd`

## Installation
1. Clone the repository:
    ```sh
    git clone https://github.com/phamthangpri/bank_reconciliation.git
    ```
2. Navigate to the repository directory:
    ```sh
    cd bank_reconciliation
    ```
3. Install the required libraries:
    ```sh
    pip install pandas numpy re openpyxl xlrd
    ```

## Usage
1. Prepare your bank statements and back office data in the appropriate format.
2. Configure the parameters in the script to match your data.
3. Run the reconciliation script:
    ```sh
    python reconcile.py
    ```
4. Review the generated reports to identify and resolve discrepancies.

## Scripts
- **reconcile.py**: Main script to perform the bank reconciliation.
- **utils**: Utility functions used in the reconciliation process.
- **report_generator.py**: Script to generate detailed reconciliation reports.

## Configuration
- Ensure your MT940 files and back office data are formatted correctly.
- Update the script parameters to match the column names and data formats of your files.

## Contributing
Contributions are welcome! Please fork the repository and submit a pull request with your changes.

## Author
Thi Thang Pham

## License
This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Contact
For any questions or feedback, please contact [Thi Thang Pham](https://github.com/phamthangpri).

## Bitcoin Transactions Types and their Impact on Storage Scalability

This project investigates different Bitcoin transaction types and analyzes their impact on storage scalability. It involves a large formatted file of 90 GB, which is essential for the analysis.

## Large File

Due to the file's size, it is not stored directly in this repository. However, you can access and download it using the following link. Please note that you may need to request access through Google Drive to download the file.

[Download Large File](https://drive.google.com/file/d/1o7lFv-dQJ0yTPLyZtmHa-a8_mC89HCHC)

## Using the Large File

This section provides a description of how the large formatted file should be used, what information it contains, and any other relevant details.

### Requirements

- **Python:** Recommended version 3.6 or higher. You can download it from [python.org](https://www.python.org/downloads/).
- **Additional Libraries:** Depending on the file format and your processing needs, you might need additional libraries, such as `pandas` for data manipulation and `numpy` for numerical operations.

You can install the additional libraries using pip:

```sh
pip install pandas numpy

import pandas as pd  # Import pandas library if the file is tabular and you want to use a DataFrame

# Replace 'FILE_PATH' with the actual path to the downloaded file
file_path = 'FILE_PATH'

# Open the file and read its content
with open(file_path, 'r') as file:
    content = file.read()

# Process the content as needed
# For example, if the content is tabular, you can load it into a DataFrame
# data = pd.read_csv(file_path)

# Display the first few rows of the DataFrame, if applicable
# print(data.head())


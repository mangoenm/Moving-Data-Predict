import pandas as pd
import numpy as np
import os

source_path = "/home/ubuntu/moving_big_data/Stocks/"
save_path = "/home/ubuntu/moving_big_data/Output/"
index_file_path = "/home/ubuntu/moving_big_data/CompanyNames/top_companies.txt"

#source_path = "opt/airflow/moving_big_data/Stocks/"
#save_path = "opt/airflow/moving_big_data/Output/"
#index_file_path = "opt/airflow/moving_big_data/CompanyNames/top_companies.txt"

#Create a function that returns a list of all the companies (represented by their csv files) selected for inclusion within the data processing pipeline.

def extract_companies_from_index(index_file_path):
    """Generate a list of company files that need to be processed. 

    Args:
        index_file_path (str): path to index file

    Returns:
        list: Names of company names. 
    """
    company_file = open(index_file_path, "r")
    contents = company_file.read()
    contents = contents.replace("'","")
    contents_list = contents.split(",")
    cleaned_contents_list = [item.strip() for item in contents_list]
    company_file.close()
    return cleaned_contents_list

#Create a function that attaches the source directory to each company csv file selected for processing

def get_path_to_company_data(list_of_companies, source_data_path):
    """Creates a list of the paths to the company data
       that will be processed

    Args:
        list_of_companies (list): Extracted `.csv` file names of companies whose data needs to be processed.
        source_data_path (str): Path to where the company `.csv` files are stored. 

    Returns:
        [type]: [description]
    """
    path_to_company_data = []
    for file_name in list_of_companies:
        path_to_company_data.append(source_data_path + file_name)
    return path_to_company_data

#Create a function that saves a pandas dataframe in csv format

def save_table(dataframe, output_path, file_name, header):
    """Saves an input pandas dataframe as a CSV file according to input parameters.

    Args:
        dataframe (pandas.dataframe): Input dataframe.
        output_path (str): Path to which the resulting `.csv` file should be saved. 
        file_name (str): The name of the output `.csv` file. 
        header (boolean): Whether to include column headings in the output file.
    """
    print(f"Path = {output_path}, file = {file_name}")
    dataframe.to_csv(output_path + file_name + ".csv", index=False, header=False)

def data_processing(file_paths, output_path):
    """Process and collate company csv file data for use within the data processing component of the formed data pipeline.  

    Args:
        file_paths (list[str]): A list of paths to the company csv files that need to be processed. 
        output_path (str): The path to save the resulting csv file to.  
    """
    
    count_successful = 0
    count_failed = 0
    successful_files = []

    # INSERT YOUR CODE HERE
    combined_df = pd.DataFrame()
    
    for file_path in path_to_company_data:
        #print(file_path)
        #df = pd.read_csv(file_path)
        if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
            # Read the CSV file into a DataFrame
            df = pd.read_csv(file_path)
            file_name = os.path.basename(file_path)


            df['daily_percent_change'] = ((df['Close'] - df['Open']) /  df['Open']) * 100


            df['value_change'] = df['Close'] - df['Open']

            #file_path.split('/')
            parts = file_name.split('.')
            company_name = parts[0]
            df['company_name'] = company_name


            df.rename(columns={df.columns[0]: 'stock_date', df.columns[1]: 'open_value', df.columns[2]: 'high_value', 
                      df.columns[3]: 'low_value', df.columns[4]: 'close_value', df.columns[5]: 'volume_traded'}, inplace=True)

            # Convert columns to specified data types
            df['stock_date'] = pd.to_datetime(df['stock_date'])
            df['open_value'] = df['open_value'].astype(np.float64)
            df['high_value'] = df['high_value'].astype(np.float64)
            df['low_value'] = df['low_value'].astype(np.float64)
            df['close_value'] = df['close_value'].astype(np.float64)
            df['volume_traded'] = df['volume_traded'].astype(np.int64)
            df['daily_percent_change'] = df['daily_percent_change'].astype(np.float64)
            df['value_change'] = df['value_change'].astype(np.int64)
            df['company_name'] = df['company_name'].astype(str)  
            df_excluded = df.drop(columns=['OpenInt'])
    
            # Append the DataFrame to the combined DataFrame
            combined_df = pd.concat([combined_df, df_excluded], ignore_index=True)
        #else:
            #print(f"File '{file_path}' is empty or does not exist.")
           # df = pd.read_csv(file_path)

        
        
            successful_files.append(file_path)
        
    # If no non-empty files were found, print a message
    if not successful_files:
        count_failed += 1
    else:
        count_successful += 1
            
    # Create the DataFrame
    final_df = pd.DataFrame(combined_df)
    return final_df


if __name__ == "__main__":

    # Get all file names in source data directory of companies whose data needs to be processed, 
    # This information is specified within the `top_companies.txt` file. 
    file_names = extract_companies_from_index(index_file_path)

    # Update the company file names to include path information. 
    path_to_company_data = get_path_to_company_data(file_names, source_path)

    # Process company data and create full data output
    df = data_processing(path_to_company_data, save_path)
    
   # Saves an input pandas dataframe as a CSV file according to input parameters
    save_table(df, save_path, "historical_stock_data", False)
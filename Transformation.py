import pandas as pd 
import datetime as datetime 
import datetime as dt

def run_transformation():
    df =pd.read_csv(r'C:\Users\ubior\Desktop\zipco_food_orchestraion_with_apache_airflow\clean_data.csv')
    # split the customername to firstname and lastname
    split_data= df['CustomerName'].str.split(' ',n=1, expand=True)
    df['firstname'] =split_data[0]
    df['lastname'] =split_data[1].fillna('')

    #clean the customer address
    df['CustomerAddress'] = df['CustomerAddress'].str.replace('\r\n', ' ', regex=False)
    
    df['Customer_PhoneNumber'] = df['Customer_PhoneNumber'].str.lstrip('(').str.strip(')').str.replace('.','-')
    df['Customer_PhoneNumber']=df['Customer_PhoneNumber'].str.replace(')','-').str.replace('x','-')
    
    # create the customer  dimention table
    customer = df[['firstname','lastname', 'CustomerAddress', 'Customer_PhoneNumber','CustomerEmail','CustomerFeedback']].drop_duplicates()
    customer['customer_id'] =range(1,len(df)+1)
    customer =customer[['customer_id','firstname','lastname', 'CustomerAddress', 'Customer_PhoneNumber','CustomerEmail','CustomerFeedback']].drop_duplicates()
    customer

    #create the  product dimension table
    product = df[['ProductName', 'PaymentType','UnitPrice','StoreLocation','PromotionApplied','OrderType']].drop_duplicates()
    product['product_id'] = range(1,len(product)+1)
    product=product[['product_id','ProductName', 'PaymentType','UnitPrice','StoreLocation','PromotionApplied','OrderType']]
    product

     #create the staff dimension table
    staff = df[['Staff_Name', 'Staff_Email', 'StaffPerformanceRating']].drop_duplicates().reset_index(drop=True)
    staff['staff_id'] = staff.index + 1
    staff=staff[['staff_id','Staff_Name', 'Staff_Email', 'StaffPerformanceRating']]
    staff

    #create the transaction table 
    transaction = df.merge(customer,on=['firstname','lastname', 'CustomerAddress', 'Customer_PhoneNumber','CustomerEmail','CustomerFeedback'], how ='left')\
    .merge(product, on=['ProductName', 'PaymentType','UnitPrice','StoreLocation','PromotionApplied','OrderType'], how ='left')\
            .merge(staff, on=['Staff_Name', 'Staff_Email', 'StaffPerformanceRating'], how ='left')
    transaction = transaction[['Quantity', 'UnitPrice','customer_id','product_id','staff_id','Date','TotalSales','DayOfWeek']] .drop_duplicates().reset_index(drop=True) 
    transaction['transaction_id'] = transaction.index+1  
    transaction[['transaction_id','customer_id','product_id','staff_id','Quantity','TotalSales', 'UnitPrice','Date','DayOfWeek']] .drop_duplicates()
    transaction


    #create the date dimension table
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    df['Date'] = pd.to_datetime(df['Date']).dt.date
    df['Day'] = pd.to_datetime(df['Date']).dt.day_name()
    df['Month'] = pd.to_datetime(df['Date']).dt.month_name()
    df['Year'] = pd.to_datetime(df['Date']).dt.year
    Date =df[['Date','Day','Month','Year']].drop_duplicates().reset_index(drop=True)
    Date['date_id']=Date.index+1
    Date[['date_id','Date','Day','Month','Year']]

    #load the dimensional table 
    transaction.to_csv('transaction.csv', index =False)
    customer.to_csv('customer.csv', index =False)
    product.to_csv('product.csv', index =False)
    staff.to_csv('staff.csv', index =False)
    Date.to_csv('Date.csv', index =False)
    df.to_csv('cleandata.csv',index=False)


print('data cleaing and transformation completed successfully')


        
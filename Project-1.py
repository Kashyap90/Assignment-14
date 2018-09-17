
# coding: utf-8

# In[1]:


import pandas as pd
df =pd.read_csv('https://raw.githubusercontent.com/jackiekazil/data-wrangling/master/data/chp3/data-text.csv') 
df.head(2)
df1 =pd.read_csv('https://raw.githubusercontent.com/kjam/data-wrangling-pycon/master/data/berlin_weather_oldest.csv') 
df1.head(2)


# In[2]:


# Get the Metadata from the above files.

df.info()
df1.info()


# In[3]:


# Get the row names from the above files.

df.index.values


# In[4]:


df1.index.values


# In[5]:


# Change the column name from any of the above file.

df.rename(columns = {'Indicator' : 'Indicator_ID'}, inplace=False)
df.head()


# In[6]:


# Change the column name from any of the above file and store the changes made permanently.

df.rename(columns = {'Indicator' : 'Indicator_ID'}, inplace=True)
df.head()


# In[7]:


# Change the names of multiple columns.

df.rename(columns = {'PUBLISH STATES' : 'Publication Status', 'WHO region' : 'WHO Region'}, inplace=True)
df.head()


# In[8]:


# Arrange values of a particular column in ascending order.

df.sort_values(by=['Year'], ascending=True)


# In[9]:


# Arrange multiple column values in ascending order.

df.sort_values(by=['Indicator_ID', 'Country', 'Year', 'WHO Region','Publication Status'], ascending=(True,False,True,False,True))


# In[10]:


# Make country as the first column of the dataframe.

df[pd.unique(['Country']+ df.columns.values.tolist())]


# In[11]:


# Get the column array using a variable Expected Output:

col1 = 'Country'
df[[col1]].values[:, 0]


# In[12]:


# Get the subset rows 11, 24, 37

df.iloc[[11, 24, 37]]


# In[13]:


# Get the subset rows excluding 5, 12, 23, and 56

df.drop([5, 12, 23, 56], axis= 0)


# In[14]:


# Load into users dataframe

users = pd.read_csv('https://raw.githubusercontent.com/ben519/DataWrangling/master/Data/users.csv')

users.head()


# In[16]:


# Load into sessions dataframe

sessions =pd.read_csv('https://raw.githubusercontent.com/ben519/DataWrangling/master/Data/sessions.csv')

sessions.head()


# In[17]:


# Load into products dataframe

products = pd.read_csv('https://raw.githubusercontent.com/ben519/DataWrangling/master/Data/products.csv')

products.head()


# In[18]:


# Load into transactions dataframe

transactions = pd.read_csv('https://raw.githubusercontent.com/ben519/DataWrangling/master/Data/transactions.csv')

transactions.head()


# In[19]:


print(users['Registered'].dtype)
print(users['Cancelled'].dtype)
print(sessions['SessionDate'].dtype)
print(transactions['TransactionDate'].dtype)


# In[20]:


#converting to datetime values using to_datetime method in pandas as these column values had a datatype as 'Object'.

users['Registered'] = pd.to_datetime(users['Registered'])
users['Cancelled'] = pd.to_datetime(users['Cancelled'])
sessions['SessionDate'] = pd.to_datetime(sessions['SessionDate'])
transactions['TransactionDate'] = pd.to_datetime(transactions['TransactionDate'])


# In[21]:


# Join users to transactions, keeping all rows from transactions and only matching rows from users (left join) 

df_left_trans_users = pd.merge(transactions,users,how="left", on="UserID")

df_left_trans_users


# In[22]:


# Which transactions have a UserID not in users? 

transactions[~transactions['UserID'].isin(users['UserID'])]


# In[23]:


# Join users to transactions, keeping only rows from transactions and users that match via UserID (inner join) 

df_Inner_trans_users = pd.merge(transactions,users,how="inner", on="UserID")

df_Inner_trans_users


# In[24]:


# Join users to transactions, displaying all matching rows AND all non-matching rows (full outer join) 

df_Outer_trans_users = pd.merge(transactions,users,how="outer", on="UserID")

df_Outer_trans_users


# In[25]:


# Determine which sessions occurred on the same day each user registered 

# Using Panda Merge

pd.merge(left=users,right=sessions,how="inner", left_on=['UserID','Registered'], right_on=['UserID','SessionDate'])


# In[31]:


# Build a dataset with every possible (UserID, ProductID) pair (cross join) 

#create two different dataframes with unique UserID and ProductID from users and transactions dataframe respectively.

df_userid = pd.DataFrame({"UserID":users["UserID"]})
df_Tran = pd.DataFrame({"ProductID":products["ProductID"]})

#create new column Key with value as 1 for both the dataframe as this would become the common key to be merged

df_userid['Key'] = 1
df_Tran['Key'] = 1


# In[32]:


#do a outer join on df_userid and df_Tran dataframe

df_out = pd.merge(df_userid,df_Tran,how='outer',on="Key")[['UserID','ProductID']]


# In[33]:


#final dataframe df_out which has every possible(UserID,ProductID) combination

df_out


# In[34]:


# Determine how much quantity of each product was purchased by each user 

#do a left join on the output table df_out from previous step with transactions table on the keys ['UserID','ProductID]

df_user_prod_quant = pd.merge(df_out,transactions,how='left',on=['UserID','ProductID'])

#Groupby the table on ['UserID','ProductID] and calculate the sum of Qunatity entity for each group

df_user_quantity = df_user_prod_quant.groupby(['UserID','ProductID'])['Quantity'].sum()

#reset index so that the index column will have consecutive default numbers and fill NAN values with 0

df_user_quantity.reset_index().fillna(0)


# In[36]:


# For each user, get each possible pair of pair transactions (TransactionID1,TransacationID2)

pd.merge(transactions,transactions,how='outer',on='UserID')


# In[38]:


# Join each user to his/her first occuring transaction in the transactions table 

df_usertran = pd.merge(users,transactions,how='left',on='UserID')

# craete a new dataframe df_ with all duplicates on UserID being dropped , only keeping the first entry

df_ = df_usertran.drop_duplicates(subset='UserID')

#reset the index to the default integer index.

df_ = df_.reset_index(drop=True)

#display the contents of the dataframe df_

df_


# In[39]:


# Test to see if we can drop columns


# In[40]:


# #Retieve the column list for the dataframe df_ created in problem statement 20

my_columns = list(df_.columns)

print(my_columns)


# In[43]:


#set threshold to drop NAs

list(df_.dropna(thresh=int(df_.shape[0] * .9), axis=1).columns)


# In[44]:


missing_info = list(df_.columns[df_.isnull().any()])

missing_info


# In[45]:


for col in missing_info:
    num_missing = df_[df_[col].isnull() ==True].shape[0]
    print('number missing for column {}: {}'.format(col, num_missing))


# In[46]:


for col in missing_info:
    num_missing = df_[df_[col].isnull() ==True].shape[0]
    print('number missing for column {}: {}'.format(col, num_missing))
    
for col in missing_info:
    percent_missing = df_[df_[col].isnull() ==True].shape[0] / df_.shape[0]
    print('percent missing for column {}: {}'.format(col, percent_missing))


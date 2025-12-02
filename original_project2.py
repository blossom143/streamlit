### Utility Functions
import pandas as pd
import sqlite3
from sqlite3 import Error

def create_connection(db_file, delete_db=False):
    import os
    if delete_db and os.path.exists(db_file):
        os.remove(db_file)

    conn = None
    try:
        conn = sqlite3.connect(db_file)
        conn.execute("PRAGMA foreign_keys = 1")
    except Error as e:
        print(e)

    return conn


def create_table(conn, create_table_sql, drop_table_name=None):
    
    if drop_table_name: # You can optionally pass drop_table_name to drop the table. 
        try:
            c = conn.cursor()
            c.execute("""DROP TABLE IF EXISTS %s""" % (drop_table_name))
        except Error as e:
            print(e)
    
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)
        
def execute_sql_statement(sql_statement, conn, params=None):
    cur = conn.cursor()
    if params:
      cur.executemany(sql_statement, params)
    else:
      cur.execute(sql_statement)

    rows = cur.fetchall()

    return rows

def step1_create_region_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None
    header = None
    regions = {}
    with open(data_filename) as file:
      for line in file:
        if line.strip():
          if not header:
            header = line.strip()
            continue
          region = line.strip().split('\t')[4]
          regions[region] = True

    sql_create_region = '''CREATE TABLE Region (
      RegionID INTEGER PRIMARY KEY AUTOINCREMENT,
      Region TEXT NOT NULL
    );'''

    conn = create_connection(normalized_database_filename, True)
    create_table(conn, sql_create_region, drop_table_name='Region')

    regions = [(key,) for key in regions]
    regions.sort()
    sql_insert_region='''INSERT INTO Region (Region) VALUES(?);'''
    execute_sql_statement(sql_insert_region, conn, regions)
    conn.commit()
    conn.close()

    # sql_check = '''SELECT * FROM Region;'''
    # myrows = execute_sql_statement(sql_check, conn)
    # print(myrows)

# step1_create_region_table('tests/data.csv', 'normalized.db')


def step2_create_region_to_regionid_dictionary(normalized_database_filename):
    conn = create_connection(normalized_database_filename)
    sql_get_region='''SELECT Region, RegionID FROM Region;'''
    region_tuple=execute_sql_statement(sql_get_region, conn)
    conn.close()

    dict_region = dict(region_tuple)
    #print(dict_region)
    return dict_region

def step3_create_country_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None
    dict_combo = {}
    header = None
    with open(data_filename) as file:
      for line in file:
        if line.strip():
          if not header:
            header = line.strip()
            continue
          row = line.strip().split('\t')
          dict_combo[row[3]] = row[4]

    rid = step2_create_region_to_regionid_dictionary('normalized.db')
    country_data = [(key, rid[dict_combo[key]]) for key in dict_combo]
    country_data.sort()

    sql_create_country = '''CREATE TABLE Country(
      CountryID INTEGER PRIMARY KEY AUTOINCREMENT,
      Country TEXT NOT NULL,
      RegionID INTEGER NOT NULL,
      FOREIGN KEY (RegionID) REFERENCES Region(RegionID)
    );'''
    sql_populate_country = '''INSERT INTO Country (Country, RegionID) VALUES(?,?);'''
    
    conn = create_connection(normalized_database_filename)
    create_table(conn, sql_create_country, 'Country')
    execute_sql_statement(sql_populate_country, conn, country_data)
    countries = execute_sql_statement('select * from Country;', conn)
    # print(countries)
    conn.commit()
    conn.close()
# step3_create_country_table('tests/data.csv', 'normalized.db')


def step4_create_country_to_countryid_dictionary(normalized_database_filename):
    conn = create_connection(normalized_database_filename)
    sql_get_country='''SELECT Country, CountryID FROM Country;'''
    country_tuple=execute_sql_statement(sql_get_country, conn)
    conn.close()

    dict_country = dict(country_tuple)
    #print(dict_region)
    return dict_country

        
        
def step5_create_customer_table(data_filename, normalized_database_filename):
    cid = step4_create_country_to_countryid_dictionary(normalized_database_filename)
    customers = []
    header = None
    with open(data_filename) as file:
      for line in file:
        if line.strip():
          if not header:
            header = line.strip()
            continue
          row = line.strip().split('\t')
          fname, lname = row[0].split(' ', 1)
          customers.append((fname, lname, row[1], row[2], cid[row[3]]))

    customers = sorted(customers, key=lambda x: (x[0], x[1]))
    sql_create_cust = '''CREATE TABLE Customer(
      CustomerID INTEGER PRIMARY KEY AUTOINCREMENT,
      FirstName TEXT NOT NULL,
      LastName TEXT NOT NULL,
      Address TEXT NOT NULL,
      City TEXT NOT NULL,
      CountryID INT NOT NULL,
      FOREIGN KEY (CountryID) REFERENCES Country(CountryID)
    );'''
    sql_populate_cust = '''INSERT INTO Customer (
      FirstName, LastName, Address, City, CountryID) VALUES(?,?,?,?,?);'''
    
    conn = create_connection(normalized_database_filename)
    create_table(conn, sql_create_cust, 'Customer')
    execute_sql_statement(sql_populate_cust, conn, customers)
    # countries = execute_sql_statement('select * from Customer;', conn)
    # print(countries)
    conn.commit()
    conn.close()



def step6_create_customer_to_customerid_dictionary(normalized_database_filename):
    conn = create_connection(normalized_database_filename)
    sql_get_cust='''SELECT FirstName || ' ' || LastName as Name, CustomerID FROM Customer;'''
    cust_tuple=execute_sql_statement(sql_get_cust, conn)
    conn.close()

    dict_cust = dict(cust_tuple)
    #print(dict_region)
    return dict_cust
        
def step7_create_productcategory_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None
    dict_pc = {}
    header = None
    with open(data_filename) as file:
      for line in file:
        if line.strip():
          if not header:
            header = line.strip()
            continue
          categories = line.strip().split('\t')[6].split(';')
          descriptions = line.strip().split('\t')[7].split(';')
          for cat, desc in zip(categories,descriptions):
            if cat not in dict_pc:
              dict_pc[cat] = desc

    dict_pc = sorted(dict_pc.items())
    sql_create_pc = '''CREATE TABLE ProductCategory (
      ProductCategoryID INTEGER PRIMARY KEY AUTOINCREMENT,
      ProductCategory TEXT NOT NULL,
      ProductCategoryDescription TEXT NOT NULL
    );'''

    sql_populate_pc = '''INSERT INTO ProductCategory (
      ProductCategory, ProductCategoryDescription) VALUES (?,?);'''
    
    conn = create_connection(normalized_database_filename)
    create_table(conn, sql_create_pc, 'ProductCategory')
    execute_sql_statement(sql_populate_pc, conn, dict_pc)
    conn.commit()
    conn.close()

def step8_create_productcategory_to_productcategoryid_dictionary(normalized_database_filename):
    conn = create_connection(normalized_database_filename)
    sql_get_pc='''SELECT ProductCategory, ProductCategoryID FROM ProductCategory;'''
    cust_pc=execute_sql_statement(sql_get_pc, conn)
    conn.close()

    dict_pc = dict(cust_pc)
    return dict_pc
        

def step9_create_product_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None
    header = None
    product_data = []
    p_names = {}
    pid = step8_create_productcategory_to_productcategoryid_dictionary(normalized_database_filename)
    with open(data_filename) as file:
      for line in file:
        if line.strip():
          if not header:
            header = line.strip()
            continue
          row = line.strip().split('\t')
          names = row[5].split(";")
          price = row[8].split(";")
          cat = row[6].split(";")
          for n,p,c in zip(names,price,cat):
            if n not in p_names:
                p_names[n] = True
                product_data.append((n,p,pid[c]))
                
    product_data.sort()

    sql_create_prod = '''CREATE TABLE Product (
      ProductID INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
      ProductName TEXT NOT NULL,
      ProductUnitPrice REAL NOT NULL,
      ProductCategoryID INTEGER NOT NULL,
      FOREIGN KEY (ProductCategoryID) REFERENCES ProductCategory(ProductCategoryID) 
    );'''

    sql_populate_prod = '''INSERT INTO Product 
    (ProductName, ProductUnitPrice, ProductCategoryID) VALUES (?,?,?);'''

    conn = create_connection(normalized_database_filename)
    create_table(conn,sql_create_prod, 'Product')
    table_p = execute_sql_statement(sql_populate_prod, conn, product_data)
    print(table_p)

    conn.commit()
    conn.close()


def step10_create_product_to_productid_dictionary(normalized_database_filename):
    conn = create_connection(normalized_database_filename)
    sql_get_p='''SELECT ProductName, ProductID FROM Product;'''
    cust_p=execute_sql_statement(sql_get_p, conn)
    conn.close()

    dict_p = dict(cust_p)
    return dict_p
        
        

def step11_create_orderdetail_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None
    from datetime import datetime
    orders = []
    header = None
    
    pid = step10_create_product_to_productid_dictionary(normalized_database_filename)
    cid = step6_create_customer_to_customerid_dictionary(normalized_database_filename)
    with open(data_filename) as file:
      for line in file:
        if line.strip():
          if not header:
            header = line.strip()
            continue

          row = line.strip().split('\t')
          customer = cid[row[0]]
          products = row[5].split(';')
          quantities = row[9].split(';')
          ord_dates = row[10].split(';')
          for p,q,o in zip(products, quantities, ord_dates):
            orders.append((customer, pid[p], datetime.strptime(o, '%Y%m%d').strftime('%Y-%m-%d'), int(q)))

    sql_create_orders = '''CREATE TABLE OrderDetail(
      OrderID INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
      CustomerID INTEGER NOT NULL,
      ProductID INTEGER NOT NULL,
      OrderDate INTEGER NOT NULL,
      QuantityOrdered INTEGER NOT NULL,
      FOREIGN KEY (CustomerID) REFERENCES Customer(CustomerID),
      FOREIGN KEY (ProductID) REFERENCES Product(ProductID)
    );'''

    sql_populate_orders = '''INSERT INTO OrderDetail 
      (CustomerID, ProductID, OrderDate, QuantityOrdered) VALUES (?,?,?,?);'''

    conn = create_connection(normalized_database_filename)
    create_table(conn, sql_create_orders, 'OrderDetail')
    execute_sql_statement(sql_populate_orders, conn, orders)

    conn.commit()
    conn.close()


def ex1(conn, CustomerName):
    
    # Simply, you are fetching all the rows for a given CustomerName. 
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer and Product table.
    # Pull out the following columns. 
    # Name -- concatenation of FirstName and LastName
    # ProductName
    # OrderDate
    # ProductUnitPrice
    # QuantityOrdered
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- round to two decimal places
    # HINT: USE customer_to_customerid_dict to map customer name to customer id and then use where clause with CustomerID
    cid = step6_create_customer_to_customerid_dictionary('normalized.db')
    my_cid = cid[CustomerName]
    sql_statement = f""" SELECT 
    Customer.FirstName || ' ' || Customer.LastName as Name, 
    Product.ProductName, OrderDetail.OrderDate, 
    Product.ProductUnitPrice, OrderDetail.QuantityOrdered, 
    ROUND(Product.ProductUnitPrice * OrderDetail.QuantityOrdered, 2) as Total
    FROM OrderDetail 
    JOIN Customer ON Customer.CustomerID = OrderDetail.CustomerID
    JOIN Product ON Product.ProductID = OrderDetail.ProductID
    WHERE OrderDetail.CustomerID = {my_cid}
    """
    return sql_statement

def ex2(conn, CustomerName):
    
    # Simply, you are summing the total for a given CustomerName. 
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer and Product table.
    # Pull out the following columns. 
    # Name -- concatenation of FirstName and LastName
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round to two decimal places
    # HINT: USE customer_to_customerid_dict to map customer name to customer id and then use where clause with CustomerID
    cid = step6_create_customer_to_customerid_dictionary('normalized.db')
    my_cid = cid[CustomerName]
    sql_statement = f"""
    SELECT 
    Customer.FirstName || ' ' || Customer.LastName as Name , 
    ROUND(SUM(Product.ProductUnitPrice * OrderDetail.QuantityOrdered), 2) as Total
    FROM OrderDetail 
    JOIN Customer ON OrderDetail.CustomerID = Customer.CustomerID
    JOIN Product ON OrderDetail.ProductID = Product.ProductID
    WHERE Customer.CustomerID = {my_cid}
    """
# WRITE YOUR CODE HERE
    return sql_statement

def ex3(conn):
    
    # Simply, find the total for all the customers
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer and Product table.
    # Pull out the following columns. 
    # Name -- concatenation of FirstName and LastName
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round to two decimal places
    # ORDER BY Total Descending 
    
    sql_statement = """
    SELECT 
    Customer.FirstName || ' ' || Customer.LastName as Name, 
    ROUND(SUM(Product.ProductUnitPrice * OrderDetail.QuantityOrdered), 2) as Total
    FROM OrderDetail 
    JOIN Customer ON OrderDetail.CustomerID = Customer.CustomerID
    JOIN Product ON OrderDetail.ProductID = Product.ProductID
    GROUP BY Customer.CustomerID
    ORDER BY Total DESC ;
    """
# WRITE YOUR CODE HERE
    return sql_statement

def ex4(conn):
    
    # Simply, find the total for all the region
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer, Product, Country, and 
    # Region tables.
    # Pull out the following columns. 
    # Region
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round to two decimal places
    # ORDER BY Total Descending 
    
    sql_statement = """
    SELECT Region.Region,
    ROUND(SUM(Product.ProductUnitPrice * OrderDetail.QuantityOrdered), 2) as Total
    FROM OrderDetail 
    JOIN Customer ON OrderDetail.CustomerID = Customer.CustomerID
    JOIN Product ON Product.ProductID = OrderDetail.ProductID
    JOIN Country ON Country.CountryID = Customer.CountryID
    JOIN Region ON Region.RegionID = Country.RegionID
    GROUP BY Region.RegionID
    ORDER BY Total DESC;
    """

    return sql_statement

def ex5(conn):
    
    # Simply, find the total for all the countries
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer, Product, and Country table.
    # Pull out the following columns. 
    # Country
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round
    # ORDER BY Total Descending 

    sql_statement = """
    SELECT Country.Country,
    ROUND(SUM(Product.ProductUnitPrice * OrderDetail.QuantityOrdered)) as Total
    FROM OrderDetail 
    JOIN Customer ON OrderDetail.CustomerID = Customer.CustomerID
    JOIN Product ON Product.ProductID = OrderDetail.ProductID
    JOIN Country ON Country.CountryID = Customer.CountryID
    GROUP BY Country.CountryID
    ORDER BY Total DESC;
    """

    return sql_statement

def ex6(conn):
    
    # Rank the countries within a region based on order total
    # Output Columns: Region, Country, CountryTotal, TotalRank
    # Hint: Round the the total
    # Hint: Sort ASC by Region

    sql_statement = """
    SELECT Region.Region, Country.Country, 
    ROUND(SUM(Product.ProductUnitPrice * OrderDetail.QuantityOrdered)) as CountryTotal,
    ROW_NUMBER() OVER (
      PARTITION BY Region.Region
      ORDER BY SUM(Product.ProductUnitPrice * OrderDetail.QuantityOrdered) DESC
    ) as TotalRank
    FROM OrderDetail
    JOIN Product ON OrderDetail.ProductID = Product.ProductID
    JOIN Customer ON Customer.CustomerID = OrderDetail.CustomerID
    JOIN Country ON Country.CountryID = Customer.CountryID
    JOIN Region ON Region.RegionID = Country.RegionID
    GROUP BY Region.RegionID, Country.CountryID
    ORDER BY Region.Region ASC, TotalRank ASC; 
    """

# WRITE YOUR CODE HERE
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement



def ex7(conn):
    
    # Rank the countries within a region based on order total, BUT only select the TOP country, meaning rank = 1!
    # Output Columns: Region, Country, Total, TotalRank
    # Hint: Round the the total
    # Hint: Sort ASC by Region
    # HINT: Use "WITH"

    sql_statement = """ WITH RankedTotals AS(
    SELECT Region.Region, Country.Country, 
    ROUND(SUM(Product.ProductUnitPrice * OrderDetail.QuantityOrdered)) as CountryTotal,
    ROW_NUMBER() OVER (
      PARTITION BY Region.Region
      ORDER BY SUM(Product.ProductUnitPrice * OrderDetail.QuantityOrdered) DESC
    ) as CountryRegionalRank
    FROM OrderDetail
    JOIN Product ON OrderDetail.ProductID = Product.ProductID
    JOIN Customer ON Customer.CustomerID = OrderDetail.CustomerID
    JOIN Country ON Country.CountryID = Customer.CountryID
    JOIN Region ON Region.RegionID = Country.RegionID
    GROUP BY Region.RegionID, Country.CountryID)
    SELECT Region, Country, CountryTotal, CountryRegionalRank
    FROM RankedTotals
    WHERE CountryRegionalRank = 1
    ORDER BY Region ASC;
    """
    return sql_statement

def ex8(conn):
    
    # Sum customer sales by Quarter and year
    # Output Columns: Quarter,Year,CustomerID,Total
    # HINT: Use "WITH"
    # Hint: Round the the total
    # HINT: YOU MUST CAST YEAR TO TYPE INTEGER!!!!

    sql_statement = """
    WITH SalesByDate AS (
      SELECT Customer.CustomerID,
      CAST(substr(OrderDetail.OrderDate, 1, 4) AS INTEGER) AS Year,
      'Q' || ((CAST(substr(OrderDetail.OrderDate, 6, 2) AS INTEGER)-1)/3+1) AS Quarter,
      Product.ProductUnitPrice * OrderDetail.QuantityOrdered AS EachSale
      FROM OrderDetail
      JOIN Customer ON Customer.CustomerID = OrderDetail.CustomerID
      JOIN Product ON Product.ProductID = OrderDetail.ProductID
    )
    SELECT Quarter, Year, CustomerID, 
    ROUND(SUM(EachSale)) AS Total
    FROM SalesByDate
    GROUP BY Year, Quarter, CustomerID
    ORDER BY Year ASC, Quarter ASC, CustomerID ASC;
    """
# WRITE YOUR CODE HERE
    return sql_statement

def ex9(conn):
    
    # Rank the customer sales by Quarter and year, but only select the top 5 customers!
    # Output Columns: Quarter, Year, CustomerID, Total
    # HINT: Use "WITH"
    # Hint: Round the the total
    # HINT: YOU MUST CAST YEAR TO TYPE INTEGER!!!!
    # HINT: You can have multiple CTE tables;
    # WITH table1 AS (), table2 AS ()

    sql_statement = """
    WITH SalesCTE AS (
      SELECT OrderDetail.CustomerID, 
      ROUND(SUM(Product.ProductUnitPrice * OrderDetail.QuantityOrdered)) as Total,
      CAST(substr(OrderDetail.OrderDate, 1, 4) AS INT) AS Year,
      'Q' || ((CAST(substr(OrderDetail.OrderDate, 6, 2) AS INT)-1)/3+1) AS Quarter
      FROM OrderDetail
      JOIN Product ON OrderDetail.ProductID = Product.ProductID
      GROUP BY Year, Quarter, OrderDetail.CustomerID
    ),
    RankedSales AS (
      SELECT Quarter, Year, CustomerID, Total,
      ROW_NUMBER() OVER (
        PARTITION BY Year, Quarter
        ORDER BY Total DESC
      ) AS CustomerRank
      FROM SalesCTE
    ) 
    SELECT Quarter, Year, CustomerID, Total, CustomerRank
    FROM RankedSales
    WHERE CustomerRank <=5
    ORDER BY Year ASC, Quarter ASC, Total DESC;
    """
    return sql_statement

def ex10(conn):
    
    # Rank the monthy sales
    # Output Columns: Quarter, Year, CustomerID, Total
    # HINT: Use "WITH"
    # Hint: Round the the total

    sql_statement = """
    WITH MonthlySales AS (
      SELECT
      SUM(ROUND(Product.ProductUnitPrice * OrderDetail.QuantityOrdered)) as Total,
      CAST(substr(OrderDetail.OrderDate, 6, 2) AS INT) AS MonthNum
      FROM OrderDetail
      JOIN Product ON Product.ProductID = OrderDetail.ProductID
      GROUP BY MonthNum
    )
    SELECT 
    CASE MonthNum
        WHEN 1 THEN 'January'
        WHEN 2 THEN 'February'
        WHEN 3 THEN 'March'
        WHEN 4 THEN 'April'
        WHEN 5 THEN 'May'
        WHEN 6 THEN 'June'
        WHEN 7 THEN 'July'
        WHEN 8 THEN 'August'
        WHEN 9 THEN 'September'
        WHEN 10 THEN 'October'
        WHEN 11 THEN 'November'
        WHEN 12 THEN 'December'
    END AS Month, Total, 
    ROW_NUMBER() OVER(ORDER BY Total DESC) AS TotalRank
    FROM MonthlySales
    ORDER BY TotalRank ASC;
    """

# WRITE YOUR CODE HERE
    return sql_statement

def ex11(conn):
    
    # Find the MaxDaysWithoutOrder for each customer 
    # Output Columns: 
    # CustomerID,
    # FirstName,
    # LastName,
    # Country,
    # OrderDate, 
    # PreviousOrderDate,
    # MaxDaysWithoutOrder
    # order by MaxDaysWithoutOrder desc
    # HINT: Use "WITH"; I created two CTE tables
    # HINT: Use Lag
    sql_statement = """
    WITH CustOrders AS (
      SELECT o.CustomerID, c.FirstName, c.LastName, co.Country, o.OrderDate
      FROM OrderDetail o
      JOIN Customer c ON c.CustomerID = o.CustomerID
      JOIN Country co ON co.CountryID = c.CountryID
    ),
    PrevOrders AS (
      SELECT CustomerID, FirstName, LastName, Country, OrderDate,
      LAG(OrderDate) OVER (
        PARTITION BY CustomerID
        ORDER BY OrderDate
      ) AS PreviousOrderDate,
      julianday(OrderDate) - julianday(LAG(OrderDate) OVER(
        PARTITION BY CustomerID
        ORDER BY OrderDate
      )) AS NoPurchase
      FROM CustOrders
    )
    SELECT CustomerID, FirstName, LastName, Country, OrderDate, PreviousOrderDate,
    MAX(NoPurchase) AS MaxDaysWithoutOrder
    FROM PrevOrders
    GROUP BY CustomerID
    ORDER BY MaxDaysWithoutOrder DESC;
    """
# WRITE YOUR CODE HERE
    return sql_statement
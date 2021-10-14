import csv
from io import StringIO
from collections import defaultdict
import lxml.etree as etree
import pyodbc as db
import csv


def xml_parser(xmlFile):
    with open(xmlFile) as obj:
        print('Starting Parse')
        xmlTree = etree.parse(obj)
        xmlRoot = xmlTree.getroot()

    namespaces = {'x': 'http://schemas.datacontract.org/2004/07/DataGenerator'}

    customer_list = []
    order_list = []
    line_list = []
    for customer in xmlRoot.findall('x:Customer', namespaces):
        customer_dict = {'CustomerId': 'null', 'Name': 'null', 'Email': 'null', 'Age': 'null'}
        customer_dict['CustomerId'] = customer.find('x:CustomerId', namespaces).text
        customer_dict['Email'] = customer.find('x:Email', namespaces).text
        customer_dict['Name'] = customer.find('x:Name', namespaces).text
        customer_dict['Age'] = customer.find('x:Age', namespaces).text

        for orders in customer.findall('x:Orders', namespaces):
            for order in orders.findall('x:Order', namespaces):
                order_details = {'OrderId': 'null', 'CustomerId': 'null', 'Total': 'null'}
                order_details['OrderId'] = order.find('x:OrderId', namespaces).text
                OrderId = order.find('x:OrderId', namespaces).text
                order_details['CustomerId'] = order.find('x:CustomerId', namespaces).text
                order_details['Total'] = order.find('x:Total', namespaces).text

                for lines in order.findall('x:Lines', namespaces):
                    for OrderLine in lines.findall('x:OrderLine', namespaces):
                        order_line = {'OrderLineId':'null', 'OrderId':'null', 'Qty':'null', 'Price':'null', 'LineTotal':'null','ProductId':'null'}
                        order_line['OrderLineId'] = OrderLine.find('x:OrderLineId', namespaces).text
                        order_line['Qty'] = OrderLine.find('x:Qty', namespaces).text
                        order_line['Price'] = OrderLine.find('x:Price', namespaces).text
                        order_line['LineTotal'] = OrderLine.find('x:Total', namespaces).text
                        order_line['ProductId'] = OrderLine.find('x:ProductId', namespaces).text
                        order_line['OrderId'] = OrderId
                        tuples = [v for k, v in order_line.items()]
                        line_list.append(tuples)
                        orders.clear()
        cust_tuples = [v for k, v in customer_dict.items()]
        order_tuples = [v for k, v in order_details.items()]
        customer_list.append(cust_tuples)
        order_list.append(order_tuples)
        print(customer_list)
        print(order_list)
        print(line_list)
        if len(customer_list) == 3:
            wrtieCustomersCSV(customer_list)
            sqlImport('C:/Users/abecc/Documents/GitHub/XML-Parser/customers.csv', 'Customers')
        if len(order_list) == 3:
            writeOrdersCSV(order_list)
            sqlImport('C:/Users/abecc/Documents/GitHub/XML-Parser/orders.csv', 'Orders')

        if len(line_list) == 10:
            writeOrderLinesCSV(line_list)
            sqlImport('C:/Users/abecc/Documents/GitHub/XML-Parser/orderLines.csv', 'OrderLines')




def wrtieCustomersCSV(list_data):
    with open('customers.csv', 'w') as file:
        header = ['CustomerId', 'Name', 'Email', 'Age']
        writer = csv.writer(file)
        writer.writerow(header)
        writer.writerows(list_data)

def writeOrdersCSV(list_data):
    with open('orders.csv', 'w') as file:
        header = ['OrderId', 'CustomerId', 'Total']
        writer = csv.writer(file)
        writer.writerow(header)
        writer.writerows(list_data)

def writeOrderLinesCSV(list_data):
    with open('orderLines.csv', 'w') as file:
        header = ['OrderLineId', 'OrderId', 'Qty', 'Price', 'LineTotal', 'ProductId']
        writer = csv.writer(file)
        writer.writerow(header)
        writer.writerows(list_data)


def sqlImport(csvFilePath, tableName ):
    connectionString = 'Driver={SQL Server};Server=tcp:sadserver1.database.windows.net,1433;Database=dbNW;Uid=sad666;Pwd=Ophelia?;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
    connection = db.connect(connectionString)
    cursor = connection.cursor()
    sql = 'BULK INSERT {0} FROM {1} WITH (FORMAT='CSV', FIRSTROW=2);'.format(tableName, csvFilePath)
    cursor.execute(sql)
    cursor.commit()
    cursor.close()

def sql_orders(orders):
    connectionString = 'Driver={SQL Server};Server=tcp:sadserver1.database.windows.net,1433;Database=dbNW;Uid=sad666;Pwd=Ophelia?;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
    connection = db.connect(connectionString)
    cursor = connection.cursor()
    sql = """INSERT INTO Orders (OrderId, CustomerId, Total) 
    VALUES (?, ?, ?)
    """
    print(orders)
    cursor.executemany(sql, orders)
    cursor.commit()
    cursor.close()

def sql_orderlines(orderlines):
    connectionString = 'Driver={SQL Server};Server=tcp:sadserver1.database.windows.net,1433;Database=dbNW;Uid=sad666;Pwd=Ophelia?;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
    connection = db.connect(connectionString)
    cursor = connection.cursor()
    sql = """INSERT INTO OrderLines (OrderLineId, OrderId, Qty, Price, LineTotal, ProductId) 
    VALUES (?, ?, ?, ?, ?, ?)
    """
    print(orderlines)
    cursor.executemany(sql, orderlines)
    cursor.commit()
    cursor.close()





def testConn():
    connectionString = 'Driver={SQL Server};Server=tcp:sadserver1.database.windows.net,1433;Database=dbNW;Uid=sad666;Pwd=Ophelia?;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
    connection = db.connect(connectionString)
    cursor = connection.cursor()
    connStr = cursor.execute('SELECT 1')
    print(connStr)


if __name__ == "__main__":
    # testConn()
    # xmlFile = open('customers.xml', 'r')
    xml_parser('sample.xml')
    # sql_dbms(customers)

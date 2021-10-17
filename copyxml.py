import csv
from io import StringIO
from collections import defaultdict
import lxml.etree as etree
import pyodbc as db


customer_list = []
order_list = []
line_list = []

def xml_parser(xmlFile):
    global customer_list
    global line_list
    global order_list

    with open(xmlFile) as obj:
        print('Starting Parse')
        xmlTree = etree.parse(obj)
        xmlRoot = xmlTree.getroot()

    namespaces = {'x': 'http://schemas.datacontract.org/2004/07/DataGenerator'}
    ImportCount = 0

    print('building lists')
    for customer in xmlRoot.findall('x:Customer', namespaces):
        customer_dict = {'CustomerId': 'null', 'Name': 'null', 'Email': 'null', 'Age': 'null'}
        customer_dict['CustomerId'] = customer.find('x:CustomerId', namespaces).text
        customer_dict['Email'] = customer.find('x:Email', namespaces).text
        customer_dict['Name'] = customer.find('x:Name', namespaces).text
        customer_dict['Age'] = customer.find('x:Age', namespaces).text
        cust_tuples = [v for k, v in customer_dict.items()]
        customer_list.append(cust_tuples)

        for orders in customer.findall('x:Orders', namespaces):
            for order in orders.findall('x:Order', namespaces):
                order_details = {'OrderId': 'null', 'CustomerId': 'null', 'Total': 'null'}
                order_details['OrderId'] = order.find('x:OrderId', namespaces).text
                OrderId = order.find('x:OrderId', namespaces).text
                order_details['CustomerId'] = order.find('x:CustomerId', namespaces).text
                order_details['Total'] = order.find('x:Total', namespaces).text
                order_tuples = [v for k, v in order_details.items()]
                order_list.append(order_tuples)


                for lines in order.findall('x:Lines', namespaces):
                    for OrderLine in lines.findall('x:OrderLine', namespaces):
                        order_line = {'OrderLineId': 'null', 'OrderId': 'null', 'Qty': 'null', 'Price': 'null',
                                      'LineTotal': 'null', 'ProductId': 'null'}
                        order_line['OrderLineId'] = OrderLine.find('x:OrderLineId', namespaces).text
                        order_line['Qty'] = OrderLine.find('x:Qty', namespaces).text
                        order_line['Price'] = OrderLine.find('x:Price', namespaces).text
                        order_line['LineTotal'] = OrderLine.find('x:Total', namespaces).text
                        order_line['ProductId'] = OrderLine.find('x:ProductId', namespaces).text
                        order_line['OrderId'] = OrderId
                        tuples = [v for k, v in order_line.items()]
                        line_list.append(tuples)
                        OrderLine.clear()
                        del OrderLine, order_line, tuples
                    lines.clear()
                    del lines
                order.clear()
                del order, order_tuples
            orders.clear()
            del orders

        if len(customer_list) > 1000:
            sql_customers(customer_list)
            customer_list.clear()
            ImportCount += 1
            print(ImportCount)

        if len(order_list) > 1000:
            sql_orders(order_list)
            order_list.clear()
            ImportCount += 1
            print(ImportCount)

        if len(line_list) > 1000:
            sql_orderlines(line_list)
            line_list.clear()
            ImportCount += 1
            print(ImportCount)

        customer.clear()
        del customer_dict, cust_tuples


def sql_customers(customers):
    connectionString = 'Driver={SQL Server};Server=tcp:sadserver1.database.windows.net,1433;Database=dbNW;Uid=sad666;Pwd=Ophelia?;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
    connection = db.connect(connectionString)
    cursor = connection.cursor()
    sql = """INSERT INTO Customers (CustomerId, Name, Email, Age) VALUES (?, ?, ?, ?)"""
    print('starting customers import')
    cursor.fast_executemany = True
    cursor.executemany(sql, customers)
    print('Import Complete')
    cursor.commit()
    cursor.close()
    connection.close()


def sql_orders(orders):
    connectionString = 'Driver={SQL Server};Server=tcp:sadserver1.database.windows.net,1433;Database=dbNW;Uid=sad666;Pwd=Ophelia?;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
    connection = db.connect(connectionString)
    cursor = connection.cursor()
    sql = """INSERT INTO Orders (OrderId, CustomerId, Total) VALUES (?, ?, ?)"""
    print('Starting orders import')
    cursor.fast_executemany = True
    cursor.executemany(sql, orders)
    print('Import Complete')
    cursor.commit()
    cursor.close()
    connection.close()


def sql_orderlines(orderlines):
    connectionString = 'Driver={SQL Server};Server=tcp:sadserver1.database.windows.net,1433;Database=dbNW;Uid=sad666;Pwd=Ophelia?;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
    connection = db.connect(connectionString)
    cursor = connection.cursor()
    sql = """INSERT INTO OrderLines (OrderLineId, OrderId, Qty, Price, LineTotal, ProductId) VALUES (?, ?, ?, ?, ?, ?)"""
    print('Starting order lines import')
    cursor.fast_executemany = True
    cursor.executemany(sql, orderlines)
    print('Import Complete')
    cursor.commit()
    cursor.close()
    connection.close()


def testConn():
    connectionString = 'Driver={SQL Server};Server=tcp:sadserver1.database.windows.net,1433;Database=dbNW;Uid=sad666;Pwd=Ophelia?;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
    connection = db.connect(connectionString)
    cursor = connection.cursor()
    connStr = cursor.execute('SELECT 1')
    print(connStr)


if __name__ == "__main__":
    # testConn()
    # xmlFile = open('customers.xml', 'r')
    xml_parser('customers.xml')
    # sql_dbms(customers)

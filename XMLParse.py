import csv
from io import StringIO
from collections import defaultdict
import lxml.etree as etree
import pyodbc as db


def xml_parser(xmlFile):
    with open(xmlFile) as obj:
        print('Starting Parse')
        xmlTree = etree.parse(obj)
        xmlRoot = xmlTree.getroot()

    namespaces = {'x': 'http://schemas.datacontract.org/2004/07/DataGenerator'}

    customers = []

    for customer in xmlRoot.findall('x:Customer', namespaces):
        customerAll = {'customer_detail': 'null', 'order_details': [], 'order_line': []}
        customer_details = {'CustomerId': 'null', 'Name': 'null', 'Email': 'null', 'Age': 'null'}
        customer_details['CustomerId'] = customer.find('x:CustomerId', namespaces).text
        customer_details['Email'] = customer.find('x:Email', namespaces).text
        customer_details['Name'] = customer.find('x:Name', namespaces).text
        customer_details['Age'] = customer.find('x:Age', namespaces).text
        customerAll['customer_detail'] = customer_details

        for orders in customer.findall('x:Orders', namespaces):
            for order in orders.findall('x:Order', namespaces):
                order_details = {'OrderId': 'null', 'CustomerId': 'null', 'Total': 'null'}
                order_details['OrderId'] = order.find('x:OrderId', namespaces).text
                OrderId = order.find('x:OrderId', namespaces).text

                order_details['CustomerId'] = order.find('x:CustomerId', namespaces).text
                order_details['Total'] = order.find('x:Total', namespaces).text
                customerAll['order_details'].append(order_details)

                for lines in order.findall('x:Lines', namespaces):
                    for OrderLine in lines.findall('x:OrderLine', namespaces):
                        order_line = {}
                        order_line['OrderLineId'] = OrderLine.find('x:OrderLineId', namespaces).text
                        order_line['Qty'] = OrderLine.find('x:Qty', namespaces).text
                        order_line['Price'] = OrderLine.find('x:Price', namespaces).text
                        order_line['LineTotal'] = OrderLine.find('x:Total', namespaces).text
                        order_line['ProductId'] = OrderLine.find('x:ProductId', namespaces).text
                        order_line['OrderId'] = OrderId
                        customerAll['order_line'].append(order_line)
                        orders.clear()

        customers.append(customerAll)

        if len(customers) == 50:
            sql_dbms(customers)
            customers = []
    print(customers)


def sql_dbms(customers):
    connectionString = 'Driver={SQL Server};Server=tcp:sadserver1.database.windows.net,1433;Database=dbNW;Uid=sad666;Pwd=Ophelia?;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
    sqlInsert = 'BULK INSERT dbName FROM {0} WITH('.format(customers)
    connection = db.connect(connectionString)
    cursor = connection.cursor()
    cursor.execute('SELECT 1')

    for customerData in customers:
        customer = []
        customer.append(customerData)

        for att in customer:
            cursor.execute(sqlInsert.format('CustomerId', att))

        for i in cursor:
            print(i)


def testConn():
    connectionString = 'Driver={SQL Server};Server=tcp:sadserver1.database.windows.net,1433;Database=dbNW;Uid=sad666;Pwd=Ophelia?;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
    connection = db.connect(connectionString)
    cursor = connection.cursor()
    connStr = cursor.execute('SELECT 1')
    print(connStr)

if __name__ == "__main__":
    #testConn()
    # xmlFile = open('customers.xml', 'r')
    xml_parser('sample.xml')
    # sql_dbms(customers)




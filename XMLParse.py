import csv
from io import StringIO
import lxml.etree as etree
import pyodbc as db


def xml_parser(xmlFile):
    with open(xmlFile) as obj:
        print('Starting Parse')
        xmlTree = etree.parse(obj)
        xmlRoot = xmlTree.getroot()


    customers_list = []
    orderLine_List = []
    orderId = 'null'
    #Goes through each customer Element
    for customer in xmlRoot:
        customer_details = {'CustomerId': 'null', 'Name': 'null', 'Email': 'null', 'Age': 'null'}
        orders = {'OrderId': 'null', 'CustomerId': 'null', 'Total': 'null'}


        #Loops through each customer attribute
        for element in customer:
            if 'CustomerId' in element.tag:
                customer_details['CustomerId'] = element.text

            if 'Name' in element.tag:
                customer_details['Name'] = element.text

            if 'Email' in element.tag:
                customer_details['Email'] = element.text

            if 'Age' in element.tag:
                customer_details['Age'] = element.text

            if 'Orders' in element.tag:
                for ordersxml in element:

                    if 'Order' in ordersxml.tag:
                        for order in ordersxml:


                            #order_lines['OrderId'] = orders['OrderId']
                            if 'Lines' in order.tag:
                                for line in order:
                                    order_lines = {'OrderLineId': 'null', 'OrderId': 'null', 'Qty': 'null','Price': 'null','LineTotal': 'null', 'ProductId': 'null'}
                                    for orderLine in line:

                                        if 'OrderLineId' in orderLine.tag:
                                            order_lines['OrderLineId'] = orderLine.text
                                            print(orderLine.text)

                                        if 'Price' in orderLine.tag:
                                            order_lines['Price'] = orderLine.text
                                            print(orderLine.text)

                                        if 'ProductId' in orderLine.tag:
                                            order_lines['ProductId'] = orderLine.text
                                            print(orderLine.text)

                                        if 'Qty' in orderLine.tag:
                                            order_lines['Qty'] = orderLine.text
                                            print(orderLine.text)

                                        if 'Total' in orderLine.tag:
                                            order_lines['LineTotal'] = orderLine.text
                                            print(orderLine.text)
                                    orderLine_List.append(order_lines)
                            if 'CustomerId' in order.tag:
                                orders['CustomerId'] = order.text

                            if 'OrderId' in order.tag:
                                orders['OrderId'] = order.text
                                order_lines['OrderId'] = orders['OrderId']

                            if 'Total' in order.tag:
                                orders['Total'] = order.text


        print(orders)
        print(orderLine_List)
        print(customer_details)




def sql_dbms(customers):
    connectionString = 'Driver={SQL Server};Server=tcp:sadserver1.database.windows.net,1433;Database=dbNW;Uid=sad666;Pwd=Ophelia?;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
    sqlInsert = 'INSERT INTO Customers({}) Values({})'
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
    connStr = cursor.execute('SELECT * FROM CustomerId')
    print(connStr)

if __name__ == "__main__":
    # testConn()
    # xmlFile = open('customers.xml', 'r')
    xml_parser('sample.xml')
    # sql_dbms(customers)




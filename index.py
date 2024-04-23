import json
from datetime import datetime
import uuid
import os
import boto3
import logging
logger = logging.getLogger()
logger.setLevel("INFO")

#set env variables
#DATETIME = os.environ['GET_DATETIME']
#UUID = os.environ['GET_UUID']
#QUEUE_URL = os.environ['QUEUE_URL']
#orderId = str(uuid.uuid4())
SOURCE_EMAIL_ADDRESS = 'savita@savitasingh18.shop'
ORDER_TABLE = 'OrderDB'
PRODUCT_ORDER_QUEUE = ''
SHIPPING_QUEUE = 'https://sqs.eu-north-1.amazonaws.com/706824313294/shippingOrderQueue'
ORDER_STATUS_NEXT = "Waiting-Order Confirmation"

def lambda_handler(event, context):
    
    logger.info(event)
    # Queue Resource
    sqs = boto3.client('sqs')
    # Dynamo DB Resouce 
    dynamodb = boto3.resource('dynamodb')
    
    # SNS & SQS Resource
    clientSNS = boto3.client('sns',region_name='eu-north-1')
    logger.info ("Initialise SNS Queue")
    
    ses_client = boto3.client('ses', region_name='us-east-1')
    logger.info ("Initialise SES Service")
    
    try:
        
        #
        #
        #
        # Read messages from the Subit Order Queue.
        # 
        response = sqs.receive_message(
        QueueUrl=SHIPPING_QUEUE,
        MaxNumberOfMessages=1,  # Maximum number of messages to retrieve
        WaitTimeSeconds=0  # Wait time for long-polling (in seconds)
        )
        
        logger.info (response)
        
        for record in event['Records']:
            
            logger.info ("Reading messages from the Queue")
            logger.info (record)
            message = record['body']
            
            logger.info ("Message Content:" + message)

            messageBody = json.loads(message)
            
            logger.info ("Received message:" +  message)
            #logger.info (message)
            
            # Extract Order messageBody from the POST event
            productId = messageBody['id']
            logger.info (productId)
    
            orderId = messageBody['id']        
            name = messageBody['name']
            deviceType = messageBody['deviceType']
            brand = messageBody['brand']
            model = messageBody['model']
            screenSize = messageBody['screenSize']
            email = messageBody['email']
            customerName = messageBody['customerName']
            logger.info ("Data extracted from Order request")
    
    
            #Upload Order in the Order Database
            table = dynamodb.Table(ORDER_TABLE)

            
            # updation of items occur
            response = table.update_item(                                       
                Key={
                    'id': orderId
                },
                #UpdateExpression="set status = :p",
                #ExpressionAttributeValues={
                #    ':p': "OrderInProgress-Fulfilment"

                #},
                
                UpdateExpression='SET #st = :val1',
                    ExpressionAttributeValues={
                    ":val1": ORDER_STATUS_NEXT
                },
                ExpressionAttributeNames={
                    "#st": "status"
                },
                ReturnValues="UPDATED_NEW"
            )
            

            logger.info("Informaiton extracted from payload: " + orderId + "," + productId + "," + name + "," + deviceType)
            


            # Push Order int the Queue for further processing
            sqs = boto3.client('sqs')
            message = {
    	        'id': orderId,
    	        'productId': productId,
                'name': name,
                'deviceType' : deviceType,
                'brand' : brand,
                'model' : model,
                'screenSize' : screenSize,
                'email' : email, 
                'customerName' : customerName,
                'status' : ORDER_STATUS_NEXT
                }

            messageBody = json.dumps(message)
            logger.info(messageBody)
            #response = queue.send_message(MessageBody=messageBody)
            reponse = sqs.send_message (QueueUrl=SHIPPING_QUEUE,DelaySeconds=0,MessageBody=messageBody); 
            
            
            logger.info(response)
            
            logger.info("Order published to the queue as per the Device Type [" + deviceType + "]")


            # 
            # 
            # Send Notification to the User
            #
            emailMessage = "Dear " + customerName + " \n\n\nYou order is on your way. \n. \nOnce you receive the products, please confirm by sending reply to this email. \n\nThank You\nsavitasingh.shop"
            subject = "Order : " + orderId + "On its way to you!!!"
            
            logger.info ("Sending email");    
            response = ses_client.send_email(
            Destination={
                'ToAddresses': [email]
            },
            Message={
                'Body': {
                    'Text': {
                        'Charset': 'UTF-8',
                        'Data': emailMessage,
                    }
                },
                'Subject': {
                    'Charset': 'UTF-8',
                    'Data': subject,
                },
            },
            Source=SOURCE_EMAIL_ADDRESS
            )
            
            logger.info (response);
            logger.info ("Email sent to ", customerName)
            
            # Delete the message from the queue once processed
            receipt_handle = message['ReceiptHandle']
            #sqs.delete_message(
            #    QueueUrl=ORDER_QUEUE,
            #    ReceiptHandle=receipt_handle
            #)
            
            logger.info ("Deleting message from the Queue post processing")



    except Exception as e:
        
        logger.error("ERROR while processing ")
        logger.info(e)
        
        
        #return json.dumps(data)
        return {
            "statusCode" : 500,
            "status" : "System ERROR"
            
        }
        print(e)
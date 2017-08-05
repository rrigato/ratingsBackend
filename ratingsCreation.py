import pandas as pd
import boto3
from decimal import Decimal
class ratings:
	'''
		Loads the json file and creates a DynamoDB table to upload it
	'''
	def __init__(self):
		'''
			Initializes the ratings object
		'''
		self.loadData()

	def loadData(self):
		'''
			Loads the JSON file into memory
		'''
		self.train = pd.read_json('/home/ryan/Documents/ratingsBackend/toonamiCombindedDate.json')
		print(self.train.head())

	def awsConnect(self):
		'''
			Configures the connection to AWS
		'''
		#self.dynamodb = boto3.client('dynamodb', endpoint_url = 'https://dynamodb.us-east-1.amazonaws.com')
		#self.dynamodb = boto3.client('dynamodb')
		self.dynamodb = boto3.resource('dynamodb', endpoint_url = 'http://localhost:8000')
		#print(self.dynamodb.list_tables())
	def createTable(self):
		'''
			Creates the ddl for the table
			self.Ratings = table object that is created in dynamoDB
		'''

		self.ratingsTable= self.dynamodb.create_table(
		    TableName='Ratings',
		    KeySchema=[
			{
			    'AttributeName': 'Show',
			    'KeyType': 'HASH'  #Partition key
			},
			{
			    'AttributeName': 'Date',
			    'KeyType': 'RANGE'  #Sort key
			}
		    ],
		    AttributeDefinitions=[
			{
			    'AttributeName': 'Show',
			    'AttributeType': 'S'
			},
			{
			    'AttributeName': 'Date',
			    'AttributeType': 'S'
			}

		    ],
		    ProvisionedThroughput={
			'ReadCapacityUnits': 10,
			'WriteCapacityUnits': 10
		    }
		)
		print(self.ratingsTable.table_status)
		
		

	def insertTable(self):
		'''Inserts the json file into the dynamodb table
			
		'''
		
		'''
			Iterates over each row and inserts into dynamodb
		'''
		table = self.dynamodb.Table('Ratings')
		for index, row in self.train.iterrows():
			
			#inserts into dynamodb
			table.put_item(
				Item = {
					'Date':str(row['Date']),
					'Show':row['Show'],
					'AHousehold':str(row['AHousehold'])
				}
			)
			

		#self.ratingsTable.delete()
		
if __name__ == '__main__':
	ratingsObj = ratings()

	ratingsObj.awsConnect()
	
	ratingsObj.createTable()

	ratingsObj.insertTable()

	




#AttributeName=Show,AttributeType=S AttributeName=Date,AttributeType=S AttributeName=Time,AttributeType=S AttributeName=Household,AttributeType=S AttributeName=Total,AttributeType=S AttributeName=AHousehold,AttributeType=S AttributeName=ATotal,AttributeType=S --key-schema AttributeName=Show,KeyType=HASH AttributeName=Date,KeyType=RANGE --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5

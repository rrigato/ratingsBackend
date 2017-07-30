import pandas as pd
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
		
if __name__ == '__main__':
	ratingsObj = ratings()
	

import csv
import pymysql

class CalulateGrade(object):
	"""docstring for CalulateGrade"""
	def __init__(self, file_name):
		self.file_name = file_name
		self.conn = pymysql.connect(host = "localhost", user = "root", passwd = "", db = "grade_data")
		self.myCursor = self.conn.cursor()
		
		with open(self.file_name) as csvfile:
			column = {}
			reader = csv.reader(csvfile, delimiter=',')
			headers = next(reader)
			for h in headers:
				column[h] = []
			for row in reader:
				for h, v in zip(headers, row):
					column[h].append(v)

		self.header = headers
		self.column = column

	def create_table(self):
		self.myCursor.execute("DROP TABLE IF EXISTS grade")
		year = self.header[0]; sememter = self.header[1]; codes = self.header[2]; subject = self.header[3] 
		credit = self.header[4]; grade = self.header[5]; score = self.header[6]
		createTable = """CREATE TABLE grade(
			{} varchar(255),
			{} int,
			{} int,
			{} varchar(255),
			{} float(10),
			{} varchar(255),
			{} float(10),
			primary key({})
			)""".format(year, sememter, codes, subject, credit, grade, score, codes)
		self.myCursor.execute(createTable)
		self.conn.close()
		print("Table Created")

	def insert_data(self):

		rows = ''
		for i in range(len(self.column[self.header[0]])):
			rows += "('{}','{}','{}','{}','{}','{}','{}')".format(self.column[self.header[0]][i], 
				self.column[self.header[1]][i], self.column[self.header[2]][i], self.column[self.header[3]][i],
				self.column[self.header[4]][i], self.column[self.header[5]][i], self.column[self.header[6]][i])
			if(i != len(self.column[self.header[0]])-1):
				rows += ','

		insertData = "INSERT INTO grade VALUES"+rows

		try:
			self.myCursor.execute(insertData)
			self.conn.commit()
		except:
			self.conn.rollback()

		self.conn.close()
		print("Data Inserted")

	def show_data(self):
		showData = """SELECT * FROM grade """
		a = self.myCursor.execute(showData)
		data = self.myCursor.fetchall()
		for i in range(a):
			print(data[i])

	def show_grade(self):
		year, semester = list(set(self.column['Year'])), list(set(self.column['Semester']))
		year.sort(), semester.sort()
		for y in range(len(year)):
 			for s in range(len(semester)):
 				showGrade = """SELECT Credit, Score 
 							FROM grade WHERE Year = {} AND Semester = {}""".format(year[y], semester[s])
 				num = self.myCursor.execute(showGrade)
 				if num != 0:
 					sum_credit = 0
 					sum_score = 0
 					data = self.myCursor.fetchall()
 					for i in range(len(data)):
 						sum_credit += data[i][0]
 						sum_score += data[i][1]*data[i][0]
 					print("{}/{}: {:.2f}".format(year[y], semester[s], sum_score/sum_credit))

grade = CalulateGrade("grade.csv")
#grade.create_table()
#grade.insert_data()
grade.show_grade()
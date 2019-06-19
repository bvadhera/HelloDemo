from flask import Flask, render_template, request, url_for,  redirect
import os, json
import time
import pyodbc
import pickle
import redis

app = Flask(__name__)

server = 'mysqlserver2893.database.windows.net'
database = 'MyDb'
username = 'bvadhera'
password = 'Hariom#836255#'
driver = '{ODBC Driver 13 for SQL Server}'

cacheName1 = 'testQueryRes1'
# cacheName2 = 'testQueryRes2'
# cacheName3 = 'testQueryRes3'
# cacheName4 = 'testQueryRes4'
# cacheName5 = 'testQueryRes5'
rds = redis.StrictRedis(host='bhanu697.redis.cache.windows.net', port=6380, db=0,
                           password='TEkLn1Spq3ztJfVdPNjHaaoWokqrAq43umVZncweUYY=', ssl=True)

def db_operation(sql , count):
	print('DRIVER=' + driver + ';SERVER=' + server + ';PORT=1443;DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
	cnxn = pyodbc.connect('DRIVER=' + driver + ';SERVER=' + server+ ';PORT=1443;DATABASE=' + database+ ';UID=' + username + ';PWD=' + password)
	print(cnxn)
	cursor = cnxn.cursor()
	if count != 0:
		starttime = time.time()
		for x in range(count):
			cursor.execute(sql)
		endtime = time.time()
		duration = endtime - starttime
		return duration

	cursor.execute(sql)
	rows = cursor.fetchall()
	cursor.close()
	cnxn.close()
	print(rows)
	return rows

@app.route("/")
def home():
 	return render_template('index.html')


@app.route('/searchdepthrangeLocation',methods=['GET','POST'])
def searchdepthrangeLocation():
	longitude  = request.form['longitude ']
	frmRange = request.form['depth1']
	toRange = request.form['depth2']
	sql ="select * from ASSIGNMENT3.quake6 where quake6.depthError between {} and {} AND longitude  > {}".format(frmRange,toRange, longitude)
	print(sql)
	result = db_operation(sql,0)
	return render_template('searchdepthrangeLocation.html', results=result)


@app.route('/searchdepthrangeLocationCache',methods=['GET','POST'])
def searchdepthrangeLocationCache():
	count = (int)(request.form['count'])
	frmRange = request.form['depth1']
	toRange = request.form['depth2']
	print(type(count))
	if rds.exists(cacheName1):
		isCache = 'True'
		start_time = time.time()
		for i in range(0, count):
			results = pickle.loads(rds.get(cacheName1))
		end_time = time.time()
		rds.delete(cacheName1)
		duration = end_time - start_time
		sql = "select * from ASSIGNMENT3.quake6 where quake6.depthError between {} and {} ".format(frmRange, toRange)
		result = db_operation(sql, 0)
	else:
		isCache = 'False'
		sql ="select * from ASSIGNMENT3.quake6 where quake6.depthError between {} and {} ".format(frmRange,toRange)
		duration = db_operation(sql, count)
		rds.set(cacheName1, pickle.dumps(duration))
		result = db_operation(sql, 0)
	return render_template('searchdepthrangeLocationCache.html', results=result, isCache=isCache, time=duration)

# @app.route("/query1" ,methods=['POST','GET'])
# def query1():
# 	count=(int)(request.form['count'])
# 	print(type(count))
# 	sql = "select * from ASSIGNMENT3.quakes"
# 	duration = db_operation(sql,count)
# 	return render_template("result1.html",result = duration)
#
#
# @app.route("/query1WithCache" ,methods=['POST','GET'])
# def query11():
# 	count=(int)(request.form['count'])
# 	print(type(count))
# 	if rds.exists(cacheName1):
# 		isCache = 'True'
# 		start_time = time.time()
# 		for i in range(0,count):
# 			results = pickle.loads(rds.get(cacheName1))
# 		end_time = time.time()
# 		rds.delete(cacheName1)
# 		duration = end_time - start_time
# 	else:
# 		isCache = 'False'
# 		sql = "select * from ASSIGNMENT3.quakes"
# 		duration = db_operation(sql,count)
# 		rds.set(cacheName1, pickle.dumps(duration))
# 	return render_template('cacheresult.html',isCache=isCache, time=duration)
#
# @app.route('/query2', methods=['GET','POST'])
# def query2():
# 	km = (int)(request.form['km'])
# 	location = request.form['location']
# 	count = (int) (request.form['count'])
# 	combined_result = []
# 	header = []
# 	result = []
# 	sql =  "select * from ASSIGNMENT3.quakes where ((place LIKE %s) AND (substring(place,1,1) between '0' and %s) AND (substring(place,2,2) = 'km'))"%('\'%'+location+'%\'', km)
# 	duration = db_operation(sql,count)
# 	return render_template('result1.html', result = duration)
#
# @app.route('/query2WithCache', methods=['GET','POST'])
# def query22():
# 	km = (int)(request.form['km'])
# 	location = request.form['location']
# 	count = (int) (request.form['count'])
# 	if rds.exists(cacheName2):
# 		isCache = 'True'
# 		start_time = time.time()
# 		for i in range(0,count):
# 			results = pickle.loads(rds.get(cacheName2))
# 		end_time = time.time()
# 		rds.delete(cacheName2)
# 		duration = end_time - start_time
# 	else:
# 		isCache = 'False'
# 		sql = "select * from ASSIGNMENT3.quakes where ((place LIKE %s) AND (substring(place,1,1) between '0' and %s) AND (substring(place,2,2) = 'km'))" % (
# 		'\'%' + location + '%\'', km)
# 		duration = db_operation(sql,count)
# 		rds.set(cacheName2, pickle.dumps(duration))
# 	return render_template('cacheresult.html',isCache=isCache, time=duration)
#
#
#
# @app.route('/searchbwdatesAndMag' ,methods=['GET','POST'])
# def query3():
# 	count = (int)(request.form['count'])
# 	start = request.form['startdate']
# 	start = start[0:10]
# 	end = request.form['enddate']
# 	end = end[0:10]
# 	frmRange = request.form['mag1']
# 	toRange = request.form['mag2']
# 	print(start , end , type(start) ,  type(end))
# 	#sql = "select * from QUAKES where (to_date(substring(TIME,1,10),'YYYY-MM-DD') between %s and %s) AND (DEPTH between %s and %s)" % ('\'' + start + '\'', '\'' + end + '\'', frmRange, toRange)
# 	sql ="select * from ASSIGNMENT3.quakes where (select CONVERT(DATE, substring(quakes.TIME,1,10))) BETWEEN (SELECT CONVERT(DATE,'{}')) AND (SELECT CONVERT(DATE,'{}')) AND (quakes.MAG between {} and {})".format(start,end,frmRange,toRange)
# 	print(sql)
# 	# select * from ASSIGNMENT3.quakes where (select CONVERT(DATE, substring(quakes.TIME,1,10))) BETWEEN (SELECT CONVERT(DATE,'2019-05-08')) AND (SELECT CONVERT(DATE,'2019-05-31')) AND (quakes.DEPTH between 5 and 7)
# 	print(sql)
# 	duration = db_operation(sql, count)
# 	return render_template("result1.html", result=duration)
#
# @app.route('/searchbwdatesAndMagCache' ,methods=['GET','POST'])
# def query33():
# 	count = (int)(request.form['count'])
# 	start = request.form['startdate']
# 	start = start[0:10]
# 	end = request.form['enddate']
# 	end = end[0:10]
# 	frmRange = request.form['mag1']
# 	toRange = request.form['mag2']
# 	print(start, end, type(start), type(end))
# 	if rds.exists(cacheName3):
# 		isCache = 'True'
# 		start_time = time.time()
# 		for i in range(0,count):
# 			results = pickle.loads(rds.get(cacheName3))
# 		end_time = time.time()
# 		rds.delete(cacheName3)
# 		duration = end_time - start_time
# 	else:
# 		isCache = 'False'
# 		sql = "select * from ASSIGNMENT3.quakes where (select CONVERT(DATE, substring(quakes.TIME,1,10))) BETWEEN (SELECT CONVERT(DATE,'{}')) AND (SELECT CONVERT(DATE,'{}')) AND (quakes.MAG between {} and {})".format(
# 			start, end, frmRange, toRange)
# 		duration = db_operation(sql,count)
# 		rds.set(cacheName3, pickle.dumps(duration))
# 	return render_template('cacheresult.html',isCache=isCache, time=duration)
#
# @app.route('/getearthquakedatabetweenMag' ,methods=['GET','POST'])
# def getEarthquakes():
# 	mag1 = (int)(request.form['magnitude1'])
# 	mag2 = (int)(request.form['magnitude2'])
# 	count = (int)(request.form['count'])
# 	sql = "select * from ASSIGNMENT3.quakes where MAG between  {} AND {}".format(mag1, mag2)
# 	print(sql)
# 	duration = db_operation(sql, count)
# 	return render_template("result1.html", result=duration)
#
# @app.route('/getearthquakedatabetweenMagcache' ,methods=['GET','POST'])
# def getEarthquakesCache():
# 	mag1 = (int)(request.form['magnitude1'])
# 	mag2 = (int)(request.form['magnitude2'])
# 	count = (int)(request.form['count'])
# 	if rds.exists(cacheName4):
# 		isCache = 'True'
# 		start_time = time.time()
# 		for i in range(0,count):
# 			results = pickle.loads(rds.get(cacheName4))
# 		end_time = time.time()
# 		rds.delete(cacheName4)
# 		duration = end_time - start_time
# 	else:
# 		isCache = 'False'
# 		sql = "select * from ASSIGNMENT3.quakes where MAG between  {} AND {}".format(mag1, mag2)
# 		duration = db_operation(sql,count)
# 		rds.set(cacheName4, pickle.dumps(duration))
# 	return render_template('cacheresult.html',isCache=isCache, time=duration)
#
# @app.route('/getStateCode',methods=['GET','POST'])
# def getStateCode():
# 	statecode = request.form['statecode']
# 	starttime = time.time()
# 	# sql = "SELECT population.state, population.["+year+"] FROM population, statecode\
# 	#        where statecode.code = \'"+ code +"\' and population.state = statecode.state"
# 	sql = "SELECT state from ASSIGNMENT3.statecode where CONVERT(VARCHAR, ASSIGNMENT3.statecode.code) = CONVERT(VARCHAR, '{}')".format(statecode)
# 	print(sql)
# 	results = db_operation(sql, 0)
# 	print(results[0][0])
# 	sql = "SELECT * from ASSIGNMENT3.counties where CONVERT(VARCHAR, ASSIGNMENT3.counties.State) = CONVERT(VARCHAR, '{}')".format(results[0][0])
# 	print(sql)
# 	results = db_operation(sql, 0)
# 	print(results)
# 	# sql = "SELECT ASSIGNMENT3.counties.state, count(ASSIGNMENT3.counties.county) from ASSIGNMENT3.counties where CONVERT(VARCHAR, ASSIGNMENT3.counties.State) = CONVERT(VARCHAR, '{}')".format(results[0][0])
# 	# sql = "SELECT count(CONVERT(VARCHAR, ASSIGNMENT3.counties.county) from ASSIGNMENT3.counties where CONVERT(VARCHAR, ASSIGNMENT3.counties.State) = CONVERT(VARCHAR, '{}')".format(results[0][0])
# 	print(sql)
# 	count = db_operation(sql, 0)
# 	print(count)
# 	endtime = time.time()
# 	duration = endtime - starttime
# 	return render_template('stateCounty.html', data=results, count = count[0], time=duration)
#
# @app.route('/populationRange',methods=['GET','POST'])
# def getPopulationRange():
# 	pop1 = (int)(request.form['pop1'])
# 	pop2 = (int)(request.form['pop2'])
# 	year = (int)(request.form['year'])
# 	starttime = time.time()
# 	# select ASSIGNMENT3.population.State from ASSIGNMENT3.population where ASSIGNMENT3.population.[2011] between  713906 and 4785448
# 	sql ="select ASSIGNMENT3.population.State from ASSIGNMENT3.population where ASSIGNMENT3.population.[{}] between  {} and {}".format(year,pop1,pop2)
# 	print(sql)
# 	results = db_operation(sql,0)
# 	endtime = time.time()
# 	duration = endtime - starttime
# 	return render_template('populationRange.html',data=results,time=duration)
#
#
# @app.route('/populationRangeCache',methods=['GET','POST'])
# def getPopulationRangeCache():
# 	pop1 = (int)(request.form['pop1'])
# 	pop2 = (int)(request.form['pop2'])
# 	year = (int)(request.form['year'])
# 	count = (int)(request.form['count'])
# 	if rds.exists(cacheName5):
# 		isCache = 'True'
# 		start_time = time.time()
# 		for i in range(0, count):
# 			results = pickle.loads(rds.get(cacheName5))
# 		end_time = time.time()
# 		rds.delete(cacheName5)
# 		duration = end_time - start_time
# 	else:
# 		isCache = 'False'
# 		sql ="select ASSIGNMENT3.population.State from ASSIGNMENT3.population where ASSIGNMENT3.population.[{}] between  {} and {}".format(year,pop1,pop2)
# 		duration = db_operation(sql, count)
# 		rds.set(cacheName5, pickle.dumps(duration))
# 	return render_template('cacheresult.html', isCache=isCache, time=duration)



if __name__ == '__main__':
	app.run()
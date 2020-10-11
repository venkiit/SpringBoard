# Databricks notebook source
# MAGIC %md ## SQL at Scale with Spark SQL
# MAGIC 
# MAGIC Welcome to the SQL mini project. For this project, you will use the Databricks Platform and work through a series of exercises using Spark SQL. The dataset size may not be too big but the intent here is to familiarize yourself with the Spark SQL interface which scales easily to huge datasets, without you having to worry about changing your SQL queries. 
# MAGIC 
# MAGIC The data you need is present in the mini-project folder in the form of three CSV files. This data will be imported in Databricks to create the following tables under the __`country_club`__ database.
# MAGIC 
# MAGIC <br>
# MAGIC 1. The __`bookings`__ table,
# MAGIC 2. The __`facilities`__ table, and
# MAGIC 3. The __`members`__ table.
# MAGIC 
# MAGIC You will be uploading these datasets shortly into the Databricks platform to understand how to create a database within minutes! Once the database and the tables are populated, you will be focusing on the mini-project questions.
# MAGIC 
# MAGIC In the mini project, you'll be asked a series of questions. You can solve them using the databricks platform, but for the final deliverable,
# MAGIC please download this notebook as an IPython notebook (__`File -> Export -> IPython Notebook`__) and upload it to your GitHub.

# COMMAND ----------

# MAGIC %md ### Creating the Database
# MAGIC 
# MAGIC We will first create our database in which we will be creating our three tables of interest

# COMMAND ----------

# MAGIC %sql 
# MAGIC drop database if exists country_club cascade;
# MAGIC create database country_club;
# MAGIC show databases;

# COMMAND ----------

# MAGIC %md ### Creating the Tables
# MAGIC 
# MAGIC In this section, we will be creating the three tables of interest and populate them with the data from the CSV files already available to you. 
# MAGIC To get started, first upload the three CSV files to the DBFS as depicted in the following figure
# MAGIC 
# MAGIC ![](https://i.imgur.com/QcCruBr.png)
# MAGIC 
# MAGIC 
# MAGIC Once you have done this, please remember to execute the following code to build the dataframes which will be saved as tables in our database

# COMMAND ----------

# File location and type
file_location_bookings = "/FileStore/tables/Bookings.csv"
file_location_facilities = "/FileStore/tables/Facilities.csv"
file_location_members = "/FileStore/tables/Members.csv"

file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
bookings_df = (spark.read.format(file_type) 
                    .option("inferSchema", infer_schema) 
                    .option("header", first_row_is_header) 
                    .option("sep", delimiter) 
                    .load(file_location_bookings))

facilities_df = (spark.read.format(file_type) 
                      .option("inferSchema", infer_schema) 
                      .option("header", first_row_is_header) 
                      .option("sep", delimiter) 
                      .load(file_location_facilities))

members_df = (spark.read.format(file_type) 
                      .option("inferSchema", infer_schema) 
                      .option("header", first_row_is_header) 
                      .option("sep", delimiter) 
                      .load(file_location_members))

# COMMAND ----------

# MAGIC %md ### Viewing the dataframe schemas
# MAGIC 
# MAGIC We can take a look at the schemas of our potential tables to be written to our database soon

# COMMAND ----------

print('Bookings Schema')
bookings_df.printSchema()
print('Facilities Schema')
facilities_df.printSchema()
print('Members Schema')
members_df.printSchema()

# COMMAND ----------

# MAGIC %md ### Create permanent tables
# MAGIC We will be creating three permanent tables here in our __`country_club`__ database as we discussed previously with the following code

# COMMAND ----------

permanent_table_name_bookings = "country_club.Bookings"
bookings_df.write.format("parquet").saveAsTable(permanent_table_name_bookings)

permanent_table_name_facilities = "country_club.Facilities"
facilities_df.write.format("parquet").saveAsTable(permanent_table_name_facilities)

permanent_table_name_members = "country_club.Members"
members_df.write.format("parquet").saveAsTable(permanent_table_name_members)

# COMMAND ----------

# MAGIC %md ### Refresh tables and check them

# COMMAND ----------

# MAGIC %sql
# MAGIC use country_club;
# MAGIC REFRESH table bookings;
# MAGIC REFRESH table facilities;
# MAGIC REFRESH table members;
# MAGIC show tables;

# COMMAND ----------

# MAGIC %md ### Test a sample SQL query
# MAGIC 
# MAGIC __Note:__ You can use __`%sql`__ at the beginning of a cell and write SQL queries directly as seen in the following cell. Neat isn't it!

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bookings limit 3

# COMMAND ----------

# MAGIC %md #### Q1: Some of the facilities charge a fee to members, but some do not. Please list the names of the facilities that do.

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from facilities where membercost !=0

# COMMAND ----------

# MAGIC %md ####  Q2: How many facilities do not charge a fee to members?

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select count(name) from facilities where membercost  = 0

# COMMAND ----------

# MAGIC %md #### Q3: How can you produce a list of facilities that charge a fee to members, where the fee is less than 20% of the facility's monthly maintenance cost? 
# MAGIC #### Return the facid, facility name, member cost, and monthly maintenance of the facilities in question.

# COMMAND ----------

# MAGIC %sql
# MAGIC select facid, name, membercost, monthlymaintenance from facilities where membercost !=0 and ROUND(membercost * 100.0 / monthlymaintenance, 1) > 20

# COMMAND ----------

# MAGIC %md #### Q4: How can you retrieve the details of facilities with ID 1 and 5? Write the query without using the OR operator.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from facilities where facid in (1,5)

# COMMAND ----------

# MAGIC %md #### Q5: How can you produce a list of facilities, with each labelled as 'cheap' or 'expensive', depending on if their monthly maintenance cost is more than $100? 
# MAGIC #### Return the name and monthly maintenance of the facilities in question.

# COMMAND ----------

# MAGIC %sql
# MAGIC select name,monthlymaintenance, if(monthlymaintenance > 100,'Expensive','Cheap') As Label from facilities

# COMMAND ----------

# MAGIC %md #### Q6: You'd like to get the first and last name of the last member(s) who signed up. Do not use the LIMIT clause for your solution.

# COMMAND ----------

# MAGIC %sql
# MAGIC select firstname,surname from members where joindate in (select max(joindate) from members)

# COMMAND ----------

# MAGIC %md ####  Q7: How can you produce a list of all members who have used a tennis court?
# MAGIC - Include in your output the name of the court, and the name of the member formatted as a single column. 
# MAGIC - Ensure no duplicate data
# MAGIC - Also order by the member name.

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct concat(name,firstname,surname) from members, facilities inner join bookings on members.memid = bookings.memid and facilities.facid = bookings.facid where facilities.name like '%Tennis Court%'

# COMMAND ----------

# MAGIC %md #### Q8: How can you produce a list of bookings on the day of 2012-09-14 which will cost the member (or guest) more than $30? 
# MAGIC 
# MAGIC - Remember that guests have different costs to members (the listed costs are per half-hour 'slot')
# MAGIC - The guest user's ID is always 0. 
# MAGIC 
# MAGIC #### Include in your output the name of the facility, the name of the member formatted as a single column, and the cost.
# MAGIC 
# MAGIC - Order by descending cost, and do not use any subqueries.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT f.name, concat(m.firstname, ' ', m.surname) AS MemberName, CASE WHEN membercost > 30 THEN membercost ELSE guestcost END AS Cost  FROM bookings AS b  JOIN facilities AS f ON b.facid = f.facid JOIN members AS m ON b.memid = m.memid WHERE b.starttime BETWEEN '2012-09-14' and '2012-09-14 23:59:59' AND (membercost > 30 OR guestcost > 30)   ORDER BY cost DESC

# COMMAND ----------



# COMMAND ----------

# MAGIC %md #### Q9: This time, produce the same result as in Q8, but using a subquery.

# COMMAND ----------

# MAGIC %sql
# MAGIC  SELECT f.name, m.MemberName, f.cost FROM (SELECT facid, memid, starttime AS  FROM bookings WHERE starttime BETWEEN '2012-09-14' and '2012-09-14 23:59:59') AS b JOIN (SELECT facid, name, CASE WHEN membercost > 30 THEN membercost ELSE guestcost END AS cost FROM facilities  WHERE membercost > 30 OR guestcost > 30) AS f ON b.facid = f.facid JOIN (SELECT memid, concat(firstname, ' ', surname) as MemberName FROM members) AS m ON b.memid = m.memid ORDER BY cost DESC

# COMMAND ----------

# MAGIC %md #### Q10: Produce a list of facilities with a total revenue less than 1000.
# MAGIC - The output should have facility name and total revenue, sorted by revenue. 
# MAGIC - Remember that there's a different cost for guests and members!

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT f.name,round(sum(case when memid =0 then guestcost else membercost end),1) as revenue from bookings as b join  facilities as f on f.facid = b.facid group by f.name order by revenue desc

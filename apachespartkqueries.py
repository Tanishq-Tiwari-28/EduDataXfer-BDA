from pyspark.sql import SparkSession
from pyspark.sql.functions import col, size, explode, count, avg, array_contains
import time
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException


mongo_host = "localhost"
mongo_port = "27017"
mongo_user = "admin"
mongo_password = "password"
auth_db = "admin"
db_name = "university_mongodb"  
try:
    # Initialize Spark session
    spark = SparkSession.builder \
    .appName("SparkMongoDBExample") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .config("spark.mongodb.input.uri", f"mongodb://{mongo_user}:{mongo_password}@{mongo_host}:{mongo_port}/{db_name}?authSource={auth_db}") \
    .config("spark.mongodb.output.uri", f"mongodb://{mongo_user}:{mongo_password}@{mongo_host}:{mongo_port}/{db_name}?authSource={auth_db}") \
    .getOrCreate()

    print("Session created")

    

    # courses_data.show()

except AnalysisException as e:
    print("Failed to connect to MongoDB:", e)

except Exception as e:
    print("An error occurred:", e)


from pyspark.sql.functions import size




# 1. Fetching the number of students enrolled in a specific course
def get_students_in_course(course_id):
    
    start_time = time.time()
    from pyspark.sql.functions import struct

    students_in_course = spark.read.format("com.mongodb.spark.sql.DefaultSource") \
        .option("collection", "courses") \
        .option("pipeline", f"[{{'$match': {{ 'course_id': '{course_id}' }} }}]") \
        .load() \
        .select(explode("enrollments").alias("student")) \
        .select(col("student.student_id"), col("student.name")) \
        .distinct()


    students_in_course.show(truncate=True)
    
    count = students_in_course.distinct().count()

    end_time = time.time()  # End time
    execution_time = end_time - start_time
    throughput = count / execution_time if execution_time > 0 else 0

    print(f"Query 1 Execution Time: {execution_time:.2f} seconds, Count: {count}, Throughput: {throughput:.2f} records/second")

    return count  # Count the number of distinct student IDs

print(get_students_in_course(1))
print(get_students_in_course(5))


def get_avg_students_per_instructor(instructor_id):
    # Read the instructors collection to find the courses taught by the instructor
    start_time = time.time()
    instructor_courses = spark.read.format("com.mongodb.spark.sql.DefaultSource") \
        .option("collection", "instructors") \
        .option("pipeline", f"[{{'$match': {{ 'instructor_id': '{instructor_id}' }} }}]") \
        .load() \
        .select("courses_taught") \
        .first()["courses_taught"]  # Get the array of courses taught by the instructor

    # Extract course IDs from the instructor's courses_taught
    course_ids = [course["course_id"] for course in instructor_courses]

    # Read the courses collection and calculate the number of students for each course
    avg_students_per_course = spark.read.format("com.mongodb.spark.sql.DefaultSource") \
        .option("collection", "courses") \
        .option("pipeline", "[{ '$match': { '$expr': { '$in': ['$course_id', " + str(course_ids) + "] } }}]") \
        .load() \
        .select(size("enrollments").alias("num_students")) \
        .agg(avg("num_students")) \
        .first()[0]  # Get the first element from the result
    end_time = time.time()  # End time
    execution_time = end_time - start_time
    print(f"Query 2 Execution Time: {execution_time:.2f} seconds")
    return avg_students_per_course

average_students = get_avg_students_per_instructor('9')
print(f"Average students per course for instructor 'Instructor 1': {round(average_students)}")



def get_courses_in_department(department_id):
    start_time = time.time()
    courses_in_department = spark.read.format("com.mongodb.spark.sql.DefaultSource") \
        .option("collection", "courses") \
        .load() \
        .filter(F.col("department.department_id") == department_id) \
        .select("course_id", "name", "course_code")

    end_time = time.time()  # End time
    execution_time = end_time - start_time
    print(f"Query 3 Execution Time: {execution_time:.2f} seconds")
    
    return courses_in_department.collect()

print("Courses offered by department 'CSB':")
for course in get_courses_in_department("2"):
    print(f"{course['name']} ({course['course_code']})")


def get_students_per_department():
    # Read the departments collection
    start_time = time.time()
    students_per_department = spark.read.format("com.mongodb.spark.sql.DefaultSource") \
        .option("collection", "departments") \
        .load() \
        .select("department_id", "name", "students") \
        .withColumn("num_students", size("students")) \
        .select("department_id", "name", "num_students")  # Select the relevant columns
    end_time = time.time()  # End time
    execution_time = end_time - start_time
    print(f"Query 4 Execution Time: {execution_time:.2f} seconds")
    return students_per_department.collect()  # Collect the results

print("Students per department:")
for dept in get_students_per_department():
    print(f"{dept['name']}: {dept['num_students']} students")



from pyspark.sql import functions as F

def count_instructors_for_cs_courses():
    start_time = time.time()
    cs_courses = spark.read.format("com.mongodb.spark.sql.DefaultSource") \
        .option("collection", "courses") \
        .load() \
        .filter(F.col("name").startswith("CSE"))\
        .filter(F.col("category") == "CORE")
        # Filter courses starting with "CSE" and CORE 

    # Step 2: Explode the instructors array to create a row for each instructor
    instructors = cs_courses.select(F.explode("instructors").alias("instructor")) \
        .select("instructor.instructor_id")  # Select only the instructor_id

    # Step 3: Count distinct instructors
    distinct_instructors_count = instructors.distinct().count()  # Count unique instructors
    end_time = time.time()  # End time
    execution_time = end_time - start_time
    print(f"Query 5 Execution Time: {execution_time:.2f} seconds")
    return distinct_instructors_count

# Example usage
print(f"Number of distinct instructors for courses starting with 'CSE': {count_instructors_for_cs_courses()}")


from pyspark.sql.functions import size, col, desc, expr
import time

def get_top_courses_by_enrollments():
    start_time = time.time()  # Start time
    
    # First, let's fetch all courses without any limit to investigate
    all_courses = spark.read.format("com.mongodb.spark.sql.DefaultSource") \
        .option("collection", "courses") \
        .load() \
        .select("course_id", "name", "enrollments") \
        .withColumn("enrollments_count", size(col("enrollments")))
    
    # Show some statistics
    # all_courses.describe("enrollments_count").show()
    
    # Check for any null values in enrollments
    null_enrollments = all_courses.filter(col("enrollments").isNull()).count()
    print(f"Courses with null enrollments: {null_enrollments}")
    top_courses = all_courses.orderBy(desc("enrollments_count")).limit(10)
    end_time = time.time()  # End time
    execution_time = end_time - start_time
    print(f"Query 6 Execution Time: {execution_time:.2f} seconds")
    for course in top_courses.collect():
        print(f"Course ID: {course['course_id']}, Course Name: {course['name']}, Enrollments: {course['enrollments_count']}")

    return top_courses

# Run the function
get_top_courses_by_enrollments()
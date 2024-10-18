import psycopg2
from pymongo import MongoClient
from psycopg2.extras import RealDictCursor
import time
# PostgreSQL connection parameters
pg_params = {
    "dbname": "university",
    "user": "admin",
    "password": "password",
    "host": "localhost",
    "port": "5432"
}

# MongoDB connection parameters
mongo_client = MongoClient('mongodb://admin:password@localhost:27017/')
mongo_db = mongo_client['university_mongodb']

def fetch_postgres_data(query):
    conn = psycopg2.connect(**pg_params)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(query)
    data = cur.fetchall()
    cur.close()
    conn.close()
    return data
    
def etl_departments():
    start_time = time.time()
    departments = fetch_postgres_data("SELECT * FROM Departments")
    for dept in departments:
        mongo_db.departments.insert_one({
            "department_id": str(dept['department_id']),
            "name": dept['department_name'],
            "courses": [],    # Initialize empty array for courses #Transformation
            "instructors": [], # Initialize empty array for instructors #Transformation
            "students": []     # Initialize empty array for students  #Transformation
        })
    end_time = time.time()  # End time
    execution_time = end_time - start_time
    print("ETL 1 : " , execution_time)

def etl_instructors():
    start_time = time.time()
    #Extraction
    instructors = fetch_postgres_data("""        
        SELECT i.*, d.department_name 
        FROM Instructors i
        JOIN Departments d ON i.department_id = d.department_id
    """)                  


    for inst in instructors:
        instructor_id = str(inst['instructor_id']) #Transformation
        mongo_db.instructors.insert_one({    #Loading
            "instructor_id": instructor_id,
            "name": inst['name'],
            "email": inst['email'],
            "department": {
                "department_id": str(inst['department_id']),   #Transformation
                "name": inst['department_name']
            },
            "courses_taught": []  # Initialize empty array for courses taught    #Transformation
        })

        mongo_db.departments.update_one(     #Loading
            {"department_id": str(inst['department_id'])},  #Transformation
            {"$addToSet": {"instructors": {"instructor_id": instructor_id, "name": inst['name']}}}  #Transformation and loading the Data in departments as form of set of instructors to avoid duplicacy
        )
    end_time = time.time()  # End time
    execution_time = end_time - start_time
    print("ETL 2 : " , execution_time)


def etl_students():
    start_time = time.time()

    #Extraction
    students = fetch_postgres_data(""" 
        SELECT s.*, d.department_name 
        FROM Students s
        LEFT JOIN Departments d ON s.department_id = d.department_id
    """)  

    for student in students:
        dept_info = {"department_id": str(student['department_id']), "name": student['department_name']} if student['department_id'] else None
        student_id = str(student['student_id'])  #Transformation
        mongo_db.students.insert_one({     #loading
            "student_id": student_id,
            "name": student['name'],
            "email": student['email'],
            "department": dept_info,
            "enrollments": []
        })

        if dept_info:
            mongo_db.departments.update_one( 
                {"department_id": str(student['department_id'])},
                {"$addToSet": {"students": {"student_id": student_id, "name": student['name']}}}
            )
            # similar to Loading instructors
    end_time = time.time()  # End time
    execution_time = end_time - start_time
    print("ETL 3 : " , execution_time)



def etl_courses():
    start_time = time.time()

    #Extraction
    courses = fetch_postgres_data(""" 
        SELECT c.*, d.department_name 
        FROM Courses c
        JOIN Departments d ON c.department_id = d.department_id
    """)

    for course in courses:
        course_id = str(course['course_id'])   #Transformation
        mongo_db.courses.insert_one({    #Loading
            "course_id": course_id,
            "name": course['course_name'],
            "course_code": course['course_code'],
            "department": {
                "department_id": str(course['department_id']),
                "name": course['department_name']
            },
            "Category" : course['is_elective'],  #Transformation 
            "instructors": [],  # Initialize empty array for instructors    #Transformation
            "enrollments": []    # Initialize empty array for enrollments   #Transformation
        })

        mongo_db.departments.update_one(  #Loading
            {"department_id": str(course['department_id'])},
            {"$addToSet": {"courses": {"course_id": course_id, "course_name": course['course_name']}}}
        )
    end_time = time.time()  # End time
    execution_time = end_time - start_time
    print("ETL 4 : " , execution_time)



def etl_course_instructors():
    start_time = time.time()

    #Extraction
    course_instructors = fetch_postgres_data(""" 
        SELECT ci.*, c.course_name, i.name AS instructor_name 
        FROM Course_Instructors ci
        JOIN Courses c ON ci.course_id = c.course_id
        JOIN Instructors i ON ci.instructor_id = i.instructor_id
    """)
    for ci in course_instructors:
        course_id = str(ci['course_id'])      #Transformation
        instructor_id = str(ci['instructor_id']) #Transformation

        # Load the course to include the instructor
        mongo_db.courses.update_one(
            {"course_id": course_id},
            {"$addToSet": {"instructors": {"instructor_id": instructor_id, "name": ci['instructor_name']}}}
        )

        # Load the instructor to include the course taught
        mongo_db.instructors.update_one(
            {"instructor_id": instructor_id},
            {"$addToSet": {"courses_taught": {"course_id": course_id, "course_name": ci['course_name']}}}
        )
    end_time = time.time()  # End time
    execution_time = end_time - start_time
    print("ETL 5 : " , execution_time)



def etl_enrollments():
    start_time = time.time()

    #Extraction
    enrollments = fetch_postgres_data(""" 
        SELECT e.*, s.name AS student_name, c.course_name 
        FROM Enrollments e
        JOIN Students s ON e.student_id = s.student_id
        JOIN Courses c ON e.course_id = c.course_id
    """)
    for enroll in enrollments:
        student_id = str(enroll['student_id'])    #Transformation
        course_id = str(enroll['course_id'])     #Transformation

        # Update the student to include the enrollment
        mongo_db.students.update_one(
            {"student_id": student_id},
            {"$addToSet": {"enrollments": {
                "course_id": course_id,
                "course_name": enroll['course_name']
            }}}
        )

        # Update the course to include the student enrollment
        mongo_db.courses.update_one(
            {"course_id": course_id},
            {"$addToSet": {"enrollments": {
                "student_id": student_id,
                "name": enroll['student_name']
            }}}
        )
    end_time = time.time()  # End time
    execution_time = end_time - start_time
    print("ETL 6 : " , execution_time)



def clear_mongo_collections():
    
    # Clear existing data in MongoDB
    for collection in mongo_db.list_collection_names():
        mongo_db[collection].delete_many({})

def main():
    # Clear existing documents in MongoDB before migrating
    clear_mongo_collections()

    # Perform ETL
    etl_departments()
    etl_instructors()
    etl_students()
    etl_courses()
    etl_course_instructors()
    etl_enrollments()

    print("ETL process completed!")

if __name__ == "__main__":
    main()

import psycopg2
from psycopg2.extras import execute_batch
from faker import Faker
import random

# Initialize Faker
fake = Faker()
Faker.seed(random.randint(1, 10000))
random.seed(random.randint(1, 10000))

# PostgreSQL connection parameters
pg_params = {
    "dbname": "university",
    "user": "admin",
    "password": "password",
    "host": "localhost",
    "port": "5432"
}


num_instructors = 100
num_students=2000
num_courses=50

def create_tables(conn):
    with conn.cursor() as cur:
        cur.execute("""
        DROP TABLE IF EXISTS Departments CASCADE;
        CREATE TABLE IF NOT EXISTS Departments (
            department_id SERIAL PRIMARY KEY,
            department_name VARCHAR(100) NOT NULL UNIQUE
        );
        DROP TABLE IF EXISTS Instructors CASCADE;
        CREATE TABLE IF NOT EXISTS Instructors (
            instructor_id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            email VARCHAR(100) NOT NULL UNIQUE,
            department_id INT REFERENCES Departments(department_id) ON DELETE CASCADE
        );
        DROP TABLE IF EXISTS Students CASCADE; 
        CREATE TABLE IF NOT EXISTS Students (
            student_id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            email VARCHAR(100) NOT NULL UNIQUE,
            department_id INT REFERENCES Departments(department_id) ON DELETE SET NULL
        );
        DROP TABLE IF EXISTS Courses CASCADE;  
        CREATE TABLE IF NOT EXISTS Courses (
            course_id SERIAL PRIMARY KEY,
            course_name VARCHAR(100) NOT NULL,
            course_code VARCHAR(10) NOT NULL UNIQUE,
            department_id INT REFERENCES Departments(department_id) ON DELETE CASCADE,
            is_elective VARCHAR(10) NOT NULL  -- New column to indicate if a course is CORE or ELECTIVE
        );
        DROP TABLE IF EXISTS Course_Instructors CASCADE;
        CREATE TABLE IF NOT EXISTS Course_Instructors (
            course_id INT REFERENCES Courses(course_id) ON DELETE CASCADE,
            instructor_id INT REFERENCES Instructors(instructor_id) ON DELETE CASCADE,
            PRIMARY KEY (course_id, instructor_id)
        );
        DROP TABLE IF EXISTS Enrollments CASCADE;  
        CREATE TABLE IF NOT EXISTS Enrollments (
            student_id INT REFERENCES Students(student_id) ON DELETE CASCADE,
            course_id INT REFERENCES Courses(course_id) ON DELETE CASCADE,
            PRIMARY KEY (student_id, course_id)
        );
        """)
import random
from psycopg2 import connect, sql

predefined_departments = ["CSE", "CSB", "CSAI", "CSAM", "CSSS", "CSD", "ECE", "EVE"]

def generate_departments(conn):
    with conn.cursor() as cur:
        for department in predefined_departments:
            cur.execute("INSERT INTO Departments (department_name) VALUES (%s) ON CONFLICT DO NOTHING;", (department,))
    conn.commit()
    print("DONE 1")

def assign_even_distribution(items, total):
    random.shuffle(items)
    groups = [items[i::total] for i in range(total)]
    
    return groups


def generate_instructors(conn, num_instructors):
    with conn.cursor() as cur:
        department_ids = [i + 1 for i in range(len(predefined_departments))]
        instructors_per_department = assign_even_distribution(list(range(1, num_instructors + 1)), len(department_ids))

        for idx, department_group in enumerate(instructors_per_department):
            for instructor in department_group:
                cur.execute("""
                    INSERT INTO Instructors (name, email, department_id)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (email) DO NOTHING
                """, (f"Instructor_{instructor}", f"instructor_{instructor}@university.edu", department_ids[idx]))
    conn.commit()
    print("DONE 2")


def generate_students(conn, num_students):
    with conn.cursor() as cur:
        department_ids = [i + 1 for i in range(len(predefined_departments))]
        students_per_department = assign_even_distribution(list(range(1, num_students + 1)), len(department_ids))

        for idx, department_group in enumerate(students_per_department):
            for student in department_group:
                cur.execute("""
                    INSERT INTO Students (name, email, department_id)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (email) DO NOTHING
                """, (f"Student_{student}", f"student_{student}@university.edu", department_ids[idx]))
    conn.commit()
    print("DONE 3")

def generate_courses(conn, num_courses):
    with conn.cursor() as cur:
        # Retrieve department IDs and names
        cur.execute("SELECT department_id, department_name FROM Departments;")
        departments = cur.fetchall()

        for _ in range(num_courses):
            # Randomly select a department
            department_id, department_name = random.choice(departments)  

            # Generate a unique course code and name
            course_number = random.randint(100, 999)  # Generate a random three-digit number
            course_code = f"{department_name[:3].upper()}{course_number}"  # First 3 letters of the department name
            course_name = f"{department_name} Course {course_number}"  # Set a course name based on the department

            is_elective = random.choice(["CORE", "ELECTIVE"])

            # Insert into Courses table
            cur.execute("""
                INSERT INTO Courses (course_name, course_code, department_id, is_elective)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (course_code) DO NOTHING
            """, (course_name, course_code, department_id, is_elective))

    conn.commit()

def map_courses_instructors(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT course_id FROM Courses;")
        courses = [row[0] for row in cur.fetchall()]

        cur.execute("SELECT instructor_id FROM Instructors;")
        instructors = [row[0] for row in cur.fetchall()]

        for course_id in courses:
            # Randomly assign instructors from the same department for the course
            assigned_instructors = random.sample(instructors, k=random.randint(5, 10))  # Randomly assign 5 to 10 instructors
            for instructor_id in assigned_instructors:
                cur.execute("""
                    INSERT INTO Course_Instructors (course_id, instructor_id)
                    VALUES (%s, %s)
                    ON CONFLICT (course_id, instructor_id) DO NOTHING
                """, (course_id, instructor_id))  # Ignore duplicates
    conn.commit()

    print("DONE 5")

def generate_enrollments(conn):
    with conn.cursor() as cur:
        # Fetch all student IDs
        cur.execute("SELECT student_id FROM Students")
        students = [row[0] for row in cur.fetchall()]

        cur.execute("SELECT course_id FROM Courses")
        courses = [row[0] for row in cur.fetchall()]

        min_courses = 25
        max_courses = 30

        for student_id in students:
            num_courses_for_student = random.randint(min_courses, max_courses)
            student_courses = random.sample(courses, num_courses_for_student)
            for course_id in student_courses:
                # Use ON CONFLICT to ignore duplicate entries
                cur.execute("""
                    INSERT INTO Enrollments (student_id, course_id) 
                    VALUES (%s, %s)
                    ON CONFLICT (student_id, course_id) DO NOTHING
                """, (student_id, course_id))

    conn.commit()
    print("DONE 6")


def clear_tables(conn):
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS Enrollments CASCADE;")
        cur.execute("DROP TABLE IF EXISTS Course_Instructors CASCADE;")
        cur.execute("DROP TABLE IF EXISTS Courses CASCADE;")
        cur.execute("DROP TABLE IF EXISTS Students CASCADE;")
        cur.execute("DROP TABLE IF EXISTS Instructors CASCADE;")
        cur.execute("DROP TABLE IF EXISTS Departments CASCADE;")
    conn.commit()

def main():
    conn = connect(**pg_params)
    clear_tables(conn)

    # Create tables
    create_tables(conn)
    generate_departments(conn)
    generate_instructors(conn , num_instructors)
    generate_students(conn , num_students)
    generate_courses(conn , num_courses)
    map_courses_instructors(conn)
    generate_enrollments(conn)

    conn.close()

if __name__ == "__main__":
    main()

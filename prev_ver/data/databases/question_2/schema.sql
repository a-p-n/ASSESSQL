CREATE TABLE Student (
  sno Varchar PK,
  Sname Varchar Not null,
  age Integer Must be >0,
  gender char Should contain 'M' or F as values Course
);

CREATE TABLE Course (
  Cno Varchar PK,
  Cname char(10) Notnull,
  Credits Integer Student_Course Set primary key as combination of sno,cno
);

CREATE TABLE Student_Course (
  Sno Varchar Refers to sno of student table,
  Cno Varchar Refers to cno of Course table
);
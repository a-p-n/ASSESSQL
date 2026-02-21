CREATE TABLE Programmer (
  pno varchar PK,
  Pname Varchar Not null,
  Dob Date Not null,
  Doj Date Must be > dob,
  Gender Char Must contain 'M' or 'F',
  Sal Numeric Studies
);

CREATE TABLE Studies (
  pno Varchar Foreign key,
  study_place Varchar Not null,
  course Varchar,
  course_fee Numeric Software
);

CREATE TABLE Software (
  Pno Varchar Foreign key,
  Title Varchar Not null,
  development_cost integer Not null,
  selling_cost integer Must be >development_cost
);
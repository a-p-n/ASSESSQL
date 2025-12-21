CREATE TABLE EMP (
  Eno Varchar PK,
  Ename Varchar Not null,
  Basic-sal Integer Default value 5000,
  incentive Integer Should not be greater than basic_sal,
  dept_no Varchar Refers to dno of Dept table,
  mgr_id Varchar Refers to eno DEPT
);

CREATE TABLE DEPT (
  Dno Varchar PK,
  Dname Varchar Not null,
  No. of emp Integer
);
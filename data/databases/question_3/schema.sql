CREATE TABLE Supplier (
  Sno Varchar PK,
  Sname Varchar Not null,
  City Varchar Parts
);

CREATE TABLE Parts (
  Pno Varchar PK,
  Pname Varchar Should not be left blank,
  Color Char(10),
  Weight Numeric Supplier_Parts
);

CREATE TABLE Supplier_Parts (
  Sno Varchar Refers to sno of Supplier table,
  Pno Varchar Refers to pno of Parts table,
  qty Numeric Should be >0
);
// Databricks notebook source
// Importar librerías
import spark.implicits._
import org.apache.spark.sql.functions._

// COMMAND ----------

// Crear Dataframe
val simpleData = Seq(("James","Sales","NY",90000,34,10000),
    ("Michael","Sales","NY",86000,56,20000),
    ("Robert","Sales","CA",81000,30,23000),
    ("Maria","Finance","CA",90000,24,23000),
    ("Raman","Finance","CA",99000,40,24000),
    ("Scott","Finance","NY",83000,36,19000),
    ("Jen","Finance","NY",79000,53,15000),
    ("Jeff","Marketing","CA",80000,25,18000),
    ("Kumar","Marketing","NY",91000,50,21000)
  )

val df = simpleData.toDF("employee_name","department","state","salary","age","bonus")

// COMMAND ----------

// Mostrar datos
display(df)

// COMMAND ----------

// Suma de salarios por departamento
df.groupBy("department").sum("salary").show()

// COMMAND ----------

// Contar cuantos empleados hay por departamento
df.groupBy("department").count().show()

// COMMAND ----------

// Salario máximo por departamento
df.groupBy("department").max("salary").show()

// COMMAND ----------

// GroupBy para múltiples columnas
df.groupBy("department","state").sum("salary").show()

// COMMAND ----------

// Usar agg() para multiples agregaciones a la vez
df.groupBy("department")
  .agg(
    sum("salary").as("sum_salary"),
    max("salary").as("max_salary"),
    sum("bonus").as("sum_bonus"),
    max("bonus").as("max_bonus"))
  .where(col("sum_bonus") >= 50000)
.show()

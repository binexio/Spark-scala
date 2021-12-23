// Databricks notebook source
// Importar implicits Spark
import spark.implicits._

// Datos 
val data = Seq(("James","Smith","USA","CA"),
  ("Michael","Rose","USA","NY"),
  ("Robert","Williams","USA","CA"),
  ("Maria","Jones","USA","FL")
  )

// Nombres de columnas
val columns = Seq("firstname","lastname","country","state")

//Crear Dataframe
val df = spark.createDataFrame(data).toDF(columns:_*)


// COMMAND ----------

display(df)

// COMMAND ----------

// Importar SQL functions
import org.apache.spark.sql.functions.col

// Seleccionar columnas
val selectDf = df.select("country","state")

// Seleccionar columnas asignando alias
val selectDfAlias = df.select(col("country").alias("Pais"))

// Seleccionar todas las columnas
val selectDfAll = df.select("*")


// COMMAND ----------

display(selectDfAll)

// Databricks notebook source
// Ubicacion y el tipo de archivo
var file_location = "/FileStore/tables/StudentsPerformance-1.csv"
var file_type = "csv"

// Opciones de CSV
var infer_schema = "false"
var first_row_is_header = "true"
var delimiter = ","

// Aplicar opciones en la lectura del archivo CSV
var df = spark.read.format(file_type)
                    .option("inferSchema", infer_schema)
                    .option("header", first_row_is_header)
                    .option("sep", delimiter)
                    .load(file_location)
display(df)

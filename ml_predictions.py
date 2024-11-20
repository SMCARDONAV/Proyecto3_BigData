from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_json
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.functions import vector_to_array

# Iniciar la sesión de Spark
spark = SparkSession.builder.appName("RecuperacionPrediccion").getOrCreate()

# Cargar el dataset en formato Parquet desde Cloud Storage
data = spark.read.parquet("gs://p3_bucket_2/trusted/covid_processed")

# Preprocesar datos relevantes
filtered_data = data.select("edad", "sexo", "fuente_tipo_contagio", "recuperado")
filtered_data = filtered_data.withColumn("recuperado_bin", when(col("recuperado") == "Recuperado", 1).otherwise(0))

# Indexar columnas categóricas
sexo_indexer = StringIndexer(inputCol="sexo", outputCol="sexo_index")
contagio_indexer = StringIndexer(inputCol="fuente_tipo_contagio", outputCol="contagio_index")
assembler = VectorAssembler(inputCols=["edad", "sexo_index", "contagio_index"], outputCol="features")

sexo_indexer_model = sexo_indexer.fit(filtered_data)
contagio_indexer_model = contagio_indexer.fit(filtered_data)

# Imprimir los índices y sus valores correspondientes
print("Sexo Index Mapping:")
for idx, label in enumerate(sexo_indexer_model.labels):
    print(f"{label} -> {idx}")

print("\nFuente Tipo Contagio Index Mapping:")
for idx, label in enumerate(contagio_indexer_model.labels):
    print(f"{label} -> {idx}")

# Modelo de regresión logística
lr = LogisticRegression(featuresCol="features", labelCol="recuperado_bin", maxIter=10)

# Pipeline
pipeline = Pipeline(stages=[sexo_indexer, contagio_indexer, assembler, lr])

# Dividir datos en entrenamiento y prueba
train_data, test_data = filtered_data.randomSplit([0.8, 0.2], seed=42)

# Entrenar modelo
model = pipeline.fit(train_data)

# Hacer predicciones
predictions = model.transform(test_data)

# Convertir la columna 'features' a JSON
predictions = predictions.withColumn("features_array", vector_to_array(col("features")))
predictions = predictions.withColumn("features_json", to_json(col("features_array")))


output_data = predictions.select("features_json", "recuperado_bin", "prediction")

# Evaluar modelo
evaluator = BinaryClassificationEvaluator(labelCol="recuperado_bin", metricName="areaUnderROC")
auc = evaluator.evaluate(predictions)
print(f"Área bajo la curva ROC: {auc}")

# Guardar predicciones en Cloud Storage
output_data.show()
output_data.write.csv("gs://p3_bucket_3/refined/ml_predictions/", header=True, mode="overwrite")

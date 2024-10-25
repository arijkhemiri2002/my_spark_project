from pyspark.sql import SparkSession

# Créer une session Spark
spark = SparkSession.builder     .appName("Mini Data Warehouse")     .config("spark.master", "local")     .getOrCreate()

# Lecture des fichiers CSV
sales_df = spark.read.option("header", True).csv("C:/Users/arijk/Desktop/1ere MP ISIC/SID/my_spark_project/data/raw/sales.csv")
customers_df = spark.read.option("header", True).csv("C:/Users/arijk/Desktop/1ere MP ISIC/SID/my_spark_project/data/raw/customers.csv")


# Sauvegarder les fichiers dans la couche Bronze
sales_df.write.mode("overwrite").parquet("output/bronze/sales")
customers_df.write.mode("overwrite").parquet("output/bronze/customers")



# Nettoyage du DataFrame sales_df
sales_clean_df = sales_df.dropna(subset=["OrderID0", "ProductID", "Quantity", "UnitPrice", "CustomerID"]) \
    .select(
        "OrderID0", 
        "ProductID", 
        "Quantity", 
        "UnitPrice",  # Nom dans les données brutes
        "CustomerID"
    ).withColumnRenamed("UnitPrice", "Price")  # Renommer la colonne en 'Price'

# Convertir les types de données si nécessaire (par exemple, Quantity doit être un entier)
sales_clean_df = sales_clean_df.withColumn("Quantity", sales_clean_df["Quantity"].cast("integer"))
# Exemple de conversion de Price en DoubleType
sales_clean_df = sales_clean_df.withColumn("Price", sales_clean_df["Price"].cast("double"))



# Suppression des doublons
sales_clean_df = sales_clean_df.dropDuplicates()

# Nettoyage du DataFrame customers_df
customers_clean_df = customers_df.dropna(subset=["CustomerID", "ContactName", "Phone"]) \
    .select(
        "CustomerID", 
        "ContactName",  # Nom dans les données brutes
        "Phone"    # Vérifiez que cette colonne existe dans vos données
    ).withColumnRenamed("ContactName", "Name")  # Renommer la colonne en 'Name'

# Vérification de la validité des emails (assurez-vous que la colonne existe)
if "Email" in customers_df.columns:
    customers_clean_df = customers_clean_df.filter(F.col("Email").rlike(r"^[\w\.-]+@[\w\.-]+\.\w{2,4}$"))
else:
    print("La colonne 'Email' n'existe pas dans customers_df.")

# Suppression des doublons
customers_clean_df = customers_clean_df.dropDuplicates()

# Sauvegarder les données nettoyées dans la couche Silver
sales_clean_df.write.mode("overwrite").parquet("output/silver/sales_clean")
customers_clean_df.write.mode("overwrite").parquet("output/silver/customers_clean")

# Jointure des ventes et des clients
enriched_df = sales_clean_df.join(customers_clean_df, "CustomerID")

# Sauvegarder la table enrichie dans la couche Gold
enriched_df.write.mode("overwrite").parquet("output/gold/enriched_sales")

# Créer une vue temporaire pour interroger les données
enriched_df.createOrReplaceTempView("enriched_sales")

# Requête SQL pour calculer le revenu total par produit
result = spark.sql(""" 
    SELECT ProductID, SUM(Quantity * Price) as TotalRevenue 
    FROM enriched_sales 
    GROUP BY ProductID 
""")
result.show()




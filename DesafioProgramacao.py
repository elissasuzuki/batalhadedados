# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import Row
import time

# --------- DATA FRAMES EXEMPLOS --------------
df = spark.createDataFrame([
    ('id_cliente-1',  'cat-1, cat-2, cat-3'),
    ('id_cliente-2',  'cat-1, cat-4, cat-5'),
    ('id_cliente-3',  'cat-6, cat-7'),
    ('id_cliente-4',  'cat-1, cat-2, cat-7, cat-10'),
    ('id_cliente-5',  'cat-8, cat-10'),
    ('id_cliente-6',  'cat-1, cat-9, cat-10'),
    ('id_cliente-7',  'cat-1, cat-4, cat-5, cat-10'),
    ('id_cliente-8',  'cat-7, cat-9'),
    ('id_cliente-9',  'cat-1'),
    ('id_cliente-10', 'cat-1, cat-2, cat-3, cat-4, cat-5, cat-6, cat-7, cat-8, cat-10')
], ['id_cliente', 'categorias'])
df2 = spark.createDataFrame([
    ('id_cliente-1',  'cat-1, cat-2, cat-3, cat-15'),
    ('id_cliente-2',  'cat-1, cat-4, cat-5, cat-11, cat-14'),
    ('id_cliente-3',  'cat-4, cat-14, cat-15'),
    ('id_cliente-4',  'cat-1, cat-2, cat-7, cat-10'),
    ('id_cliente-5',  'cat-8, cat-10, cat-11'),
    ('id_cliente-6',  'cat-1, cat-9, cat-10, cat-11, cat-13'),
    ('id_cliente-7',  'cat-1, cat-4, cat-5, cat-10'),
    ('id_cliente-8',  'cat-7, cat-9, cat-12, cat-13, cat-14'),
    ('id_cliente-9',  'cat-2'),
    ('id_cliente-10', 'cat-1, cat-2, cat-3, cat-4, cat-5, cat-6, cat-7, cat-8, cat-10')
], ['client-id', 'categorias'])

# --------- FUNCOES -------------

# Define uma lista com todas as categorias possiveis presentes no DataFrame
def categorias_distintas(df):
  categorias = df.select(df.columns[1]).collect()
  linhas = []
  for cat in categorias:
    linhas.extend(cat[0].encode("UTF-8").split(", "))
  
  linhas = [int(l.replace(" ","").replace("cat-","")) for l in linhas]
  distinct_cat = sorted(list(set(linhas)))
  distinct_cat = ['cat-{}'.format(str(item)) for item in distinct_cat]
  distinct_cat.insert(0,'id_cliente')
  
  return (distinct_cat)

# Trata as linhas do DataFrame para o formato de entrada da função 'createDataFrame'
def gerador_tuplas(df_row, distinct_cat):
  id_cliente = df_row[0].encode('UTF-8')
  array_cat_cliente = df_row[1].split(', ')
  lista_final = (len(distinct_cat)-1)*[0]
  for cat_cliente in array_cat_cliente:
    lista_final[distinct_cat.index(cat_cliente)-1] = 1
  lista_final.insert(0, id_cliente)
  return tuple(lista_final)

# Transforma os df em uma lista de tuplas para servir de input à função 'createDataFrame'
def gerador_lista_tuplas(df, distinct_cat):
    data = []
    for row in df.collect():
        data.append(gerador_tuplas(row, distinct_cat))

    return data

# Função final, transforma o dataframe para o formato one-hot-encoded
def one_hot_encoded(data_frame):
  distinct_cat = categorias_distintas(data_frame)
  data = gerador_lista_tuplas(data_frame, distinct_cat)
  df_final = spark.createDataFrame( data , distinct_cat)
  return df_final


# -------- TESTE ----------

data_frame = df2

start = time.time()

df_final = one_hot_encoded(data_frame)
df_final.show()

print( time.time() - start )





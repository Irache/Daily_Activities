# -*- coding: utf-8 -*-
"""
Created on Fri Jul 29 13:30:35 2022

@author: Irache Garamendi Bragado
"""
import datetime
import time
import logging

from pyspark import SparkConf, SparkContext
from pyspark.rdd import RDD
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import col, lit, create_map

# module_path = os.path.abspath(
#     os.getcwd() + 'C:/Users/usuario/Documents/Máster/TFM/script')
# if module_path not in sys.path:
#     sys.path.append(module_path)

# Cuidado con los carácteres tipo acentos en los directorios
workPath = 'C:/Users/usuario/Documents/Master/TFM'
fileConcat = str(time.time())
d = str(datetime.datetime.now())

def getSparkSessionInstance(sparkConf: SparkConf) -> SparkSession:
    '''   
Función que crea la sesion de spark
    Parameters
    ----------
    sparkConf : SparkConf
        DESCRIPTION.

    Returns
    -------
    SparkSession
        DESCRIPTION.

    '''    
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession\
            .builder\
            .config(conf=sparkConf)\
            .getOrCreate()
    return globals()['sparkSessionSingletonInstance']


if __name__ == "__main__":

    # Configuración de trazas    
    # Creación del logger que muestra la información por fichero.
    # -----------------------------------------------------------------------------
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    logFile = '%s/script/countActivityDevice.log' % (workPath)
    # controlador para fichero
    logFormat = logging.Formatter('[%(asctime)s] %(levelname)-8s [%(name)s.%(funcName)-10s:%(lineno)d] %(message)s')
    fileHandler = logging.FileHandler(logFile, mode='a')
    fileHandler.setFormatter(logFormat)
    fileHandler.setLevel(logging.DEBUG)
    
    logger.addHandler(fileHandler)

    # -----------------------------------------------------------------------------
    # Spark
    sc = SparkContext(appName="Daylies Activities DigitalDataiCASA")
    ssc = StreamingContext(sc, 60)

    # Crear stream de datos de entrada a través de HDFS
    dstreamimput = ssc.textFileStream("hdfs://localhost:9000/test")

    # Separar cada línea del fichero
    rdd = dstreamimput.map(lambda x: x.split('\n'))
    logger.info("Antes de entrar: DataiCasa")

    # Convertir RDDs de datos de entrada DStream a DataFrame y contar actividad

    def process(time: datetime.datetime, rdd: RDD[str]) -> None:
        logger.info("=====Process==== %s =========" % str(time))

        try:
            logger.info("=====Transformaciones==== %s =========" % str(time))

            # Obtener una instancia de SparkSession
            spark = getSparkSessionInstance(rdd.context.getConf())

            # Convertir RDD[String] a RDD[Row] en DataFrame
            logger.info("Traza primera fila de datos de entrada:")
            logger.info(str(rdd.first()))

            # El rdd de entrada es una lista con todos los elementos, obtener el primer elemento de la lista para hacer split y obtener cada elemento
            rdd = rdd.map(lambda x: x[0])
            logger.info("Traza lambda:")
            logger.info(str(rdd.first()))

            # Separar del primer elemento los campos
            rdd = rdd.map(lambda x: x.split(','))
            logger.info("Traza split:")
            logger.info(str(rdd.first()))

            # Formatear la cabecera y convertir a row
            rowRdd = rdd.map(lambda w: Row(dateTime=w[0], idDevice=w[1], propertyDevice=w[2], valuePropertyDevice=w[3], area=w[4]))
            logger.info("Primer elemento de los datos formateados con el nombre de las columnas:")
            logger.info(rowRdd.first())
            print("Primer elemento de los datos formateados con el nombre de las columnas:")
            print(rowRdd.first())

            # Crear dataFrame
            digitalDataDataFrame = spark.createDataFrame(rowRdd)
            logger.info("iCasaDataFrame schema:")
            logger.info(digitalDataDataFrame.printSchema())
            print("iCasaDataFrame schema:")
            digitalDataDataFrame.printSchema()
            digitalDataDataFrame.show(1)

            # Convertir columnas a Map
            DFDeviceMap = digitalDataDataFrame.withColumn("propertiesMap", create_map(
                lit("propertyDevice"), col("propertyDevice"),
                lit("valuePropertyDevice"), col("valuePropertyDevice")
            )).drop("propertyDevice", "valuePropertyDevice")

            DFDeviceMap.printSchema()
            DFDeviceMap.first()


            # Activity in bedroom: sleep + dressing + other=================================
            # General
            df_bedroom_powerStatus = DFDeviceMap.filter((DFDeviceMap.propertiesMap.getItem("propertyDevice") == "powerStatus") \
                                                              & (DFDeviceMap.propertiesMap.getItem("valuePropertyDevice") == "off") \
                                                              & (DFDeviceMap["area"] == "bedroom"))     

            df_bedroom_sensedPresence = DFDeviceMap.filter((DFDeviceMap.propertiesMap.getItem("propertyDevice") == "sensedPresence") \
                                                                & (DFDeviceMap.propertiesMap.getItem("valuePropertyDevice") == "on") \
                                                                & (DFDeviceMap["area"] == "bedroom"))
    
            # Sleep
            df_bedroom_sensedPresence_sleep = DFDeviceMap.filter((DFDeviceMap.propertiesMap.getItem("propertyDevice") == "sensedPresence")
                                                                        & (DFDeviceMap.propertiesMap.getItem("valuePropertyDevice") == "on")
                                                                        & (DFDeviceMap["area"] == "sleep"))
            
            # Dressing
            df_bedroom_sensedPresence_dresssing = DFDeviceMap.filter((DFDeviceMap.propertiesMap.getItem("propertyDevice") == "sensedPresence")
                                                                            & (DFDeviceMap.propertiesMap.getItem("valuePropertyDevice") == "on")
                                                                            & (DFDeviceMap["area"] == "wardrobe"))
            df_bedroom_sensedPresence_wardrobe = DFDeviceMap.filter((DFDeviceMap.propertiesMap.getItem("propertyDevice") == "doorWindowSensor.opneningDetection")
                                                                          & (DFDeviceMap.propertiesMap.getItem("valuePropertyDevice") == "on")
                                                                          & (DFDeviceMap["area"] == "wardrobe"))
            
            
            # df_bedroom_powerStatus.show(truncate=False)
            result_activity_general_bedroom = df_bedroom_powerStatus.count() + df_bedroom_sensedPresence.count()
            result_activity_bedroom_sleep = df_bedroom_sensedPresence_sleep.count()
            result_activity_bedroom_all_dressing = df_bedroom_sensedPresence_dresssing.count() + df_bedroom_sensedPresence_wardrobe.count()
            
            # Guardar en fichero la información
            f = open("%s/resultados/Activity_bedroom_sleep_basic_house_one_day_dataset.txt" % workPath, "a")
            f.write('[%s] - Activity in bedroom light + presence sensor: %i \n' % (d, result_activity_general_bedroom))
            f.write('[%s] - Activity in bedroom presence sensor bed: %i \n' % (d, result_activity_bedroom_sleep))       
            f.write('[%s] - Activity in bedroom presence sensor wardrobe: %i \n' % (d, result_activity_bedroom_all_dressing))            
            f.close()
            
            print("Activity in bedroom sleep:%s" % result_activity_bedroom_sleep)
            print("Activity in bedroom dressing:%s" % result_activity_bedroom_all_dressing)
            # Close Activity in bedroom: sleep + dressing + other=================================
            
            
            #Activity in bathroom===============================
            df_bathroom_powerStatus = DFDeviceMap.filter((DFDeviceMap.propertiesMap.getItem("propertyDevice") == "powerStatus") \
                       & (DFDeviceMap.propertiesMap.getItem("valuePropertyDevice") == "on") \
                       & (DFDeviceMap["area"] == "bathroom"))
            
            df_bathroom_powerStatus.show(truncate=False)
            
            df_bathroom_sensedPresence = DFDeviceMap.filter((DFDeviceMap.propertiesMap.getItem("propertyDevice") == "sensedPresence") \
                       & (DFDeviceMap.propertiesMap.getItem("valuePropertyDevice") == "on") \
                       & (DFDeviceMap["area"] == "bathroom"))
            
            df_bathroom_sensedPresence.show(truncate=False)
            
            df_bathroom_sensedPresence_toilet = DFDeviceMap.filter((DFDeviceMap.propertiesMap.getItem("propertyDevice") == "sensedPresence") \
                       & (DFDeviceMap.propertiesMap.getItem("valuePropertyDevice") == "on") \
                       & (DFDeviceMap["area"] == "toilet"))
            
            df_bathroom_sensedPresence_washbasin = DFDeviceMap.filter((DFDeviceMap.propertiesMap.getItem("propertyDevice") == "sensedPresence") \
                       & (DFDeviceMap.propertiesMap.getItem("valuePropertyDevice") == "on") \
                       & (DFDeviceMap["area"] == "washbasin"))
            
            result_activity_bathroom = df_bathroom_powerStatus.count() + df_bathroom_sensedPresence.count()
            f = open("%s/resultados/Activity_bathroom_basic_house_one_day_dataset.txt" % workPath, "a")
            f.write('[%s] - Activity in bathroom light + presencesensor: %i \n' % (d, result_activity_bathroom))
            f.write('[%s] - Activity in bathroom presence sensor washbasin: %i \n' % (d, df_bathroom_sensedPresence_washbasin.count()))
            f.write('[%s] - Activity in bathroom presence sensor toilet: %i \n' % (d, df_bathroom_sensedPresence_toilet.count()))
            f.close()  
            print("Activity in bathroom:%s" % result_activity_bathroom)  

            # close Activity in bathroom===============================

            #Activity kitchen===============================
            #cooking
            df_kitchen_powerStatus_cooking = DFDeviceMap.filter((DFDeviceMap.propertiesMap.getItem("propertyDevice") == "powerStatus") \
                       & (DFDeviceMap.propertiesMap.getItem("valuePropertyDevice") == "on") \
                       & (DFDeviceMap["area"] == "kitchen"))
            
            df_kitchen_powerStatus_cooking.show(truncate=False)
            
            df_kitchen_sensedPresence_cooking = DFDeviceMap.filter((DFDeviceMap.propertiesMap.getItem("propertyDevice") == "sensedPresence") \
                       & (DFDeviceMap.propertiesMap.getItem("valuePropertyDevice") == "on") \
                       & (DFDeviceMap["area"] == "kitchen"))
            
            df_kitchen_sensedPresence_cooking.show(truncate=False)
            
            #Eating
            df_kitchen_sensedPresence_eating = DFDeviceMap.filter((DFDeviceMap.propertiesMap.getItem("propertyDevice") == "sensedPresence") \
                       & (DFDeviceMap.propertiesMap.getItem("valuePropertyDevice") == "on") \
                       & (DFDeviceMap["area"] == "chair_kitchen"))
            
            df_kitchen_sensedPresence_eating.show(truncate=False)
            
            result_activity_cooking = df_kitchen_powerStatus_cooking.count() + df_kitchen_sensedPresence_cooking.count()
            result_activity_eating = df_kitchen_sensedPresence_eating.count()
            
            f = open("%s/resultados/Activity_kitchen_basic_house_one_day_dataset.txt" % workPath, "a")
            f.write('[%s] - Activity in kitchen light + presencesensor: %i \n' % (d,  result_activity_cooking ))            
            f.write('[%s] - Activity in kitchen presesensor eating: %i \n' % (d, result_activity_eating))
            f.close()   
            
            #Close Activity kitchen===============================
            
            #Activity in livingroom ===============================
            df_livingroom_powerStatus = DFDeviceMap.filter((DFDeviceMap.propertiesMap.getItem("propertyDevice") == "powerStatus") \
                       & (DFDeviceMap.propertiesMap.getItem("valuePropertyDevice") == "on") \
                       & (DFDeviceMap["area"] == "livingroom"))
            
            df_livingroom_powerStatus.show(truncate=False)
            
            df_livingroom_sensedPresence = DFDeviceMap.filter((DFDeviceMap.propertiesMap.getItem("propertyDevice") == "sensedPresence") \
                       & (DFDeviceMap.propertiesMap.getItem("valuePropertyDevice") == "on") \
                       & (DFDeviceMap["area"] == "livingroom"))
            
            df_livingroom_sensedPresence.show(truncate=False)
            
            df_livingroom_sensedPresence_armchair = DFDeviceMap.filter((DFDeviceMap.propertiesMap.getItem("propertyDevice") == "sensedPresence") \
                       & (DFDeviceMap.propertiesMap.getItem("valuePropertyDevice") == "on") \
                       & (DFDeviceMap["area"] == "armchair"))
            
            result_activity_livingroom = df_livingroom_powerStatus.count() + df_livingroom_sensedPresence.count()+df_livingroom_sensedPresence_armchair.count()
            f = open("%s/resultados/Activity_livingroom_SetupHouseWithLights_test_one_day.txt" % workPath, "a")
            f.write('[%s] - Activity in livingroom light + presencesensor: %i \n' % (d, result_activity_livingroom))
            f.close()    
            print("Activity in livingroom:%s" % result_activity_livingroom)  
            # Close Activity in livingroom ===============================

        except BaseException:
            logger.error("=====error==== %s =========" % str(time))
           

    DFDeviceMap = rdd.foreachRDD(process)
    ssc.start()
    time.sleep(6)
    ssc.stop(stopSparkContext=True, stopGraceFully=True)
    

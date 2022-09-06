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
    logFile = '%s/script/countActivityDeviceSpark.log' % (workPath)
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
    dstreamimput = ssc.textFileStream("hdfs://localhost:9000/imput")

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

            def count_device_status_on_for_area(df, propertyDevice, area, group):
                df_final = df.filter((df.propertiesMap.getItem("propertyDevice") == propertyDevice) \
                                                              & (df.propertiesMap.getItem("valuePropertyDevice") == "on") \
                                                              & (df["area"] == area))

                result_activity = df_final.count()
                
                # Guardar en fichero la información
                f = open("%s/resultados/Count_Device_%s_Spark.txt" % (workPath, group), "a")
                f.write('[%s] - Count status device property: %s -on- in area %s : %i \n' % (d, propertyDevice, area, result_activity))
                f.close()
            
                print("Activity in %s: %s" % (area, result_activity))
                
            def count_device_value_on_for_area(df, propertyDevice, valueAlarm, area, group):
                df_final = df.filter((df.propertiesMap.getItem("propertyDevice") == propertyDevice) \
                                                              & (df["area"] == area) \
                                                              & (int(df.propertiesMap.getItem("valuePropertyDevice")) > valueAlarm))

                result_activity = df_final.count()   
                print("Activity in %s: %s" % (area, result_activity))
                    
                # Guardar en fichero la información
                f = open("%s/resultados/Count_Device_%s_Spark.txt" % (workPath, group), "a")
                f.write('[%s] - Count value property: %s device on in area %s : %i \n' % (d, propertyDevice, area, result_activity))
                f.close()

            logger.info("contador de dispositivo bedroom. count_device_status_on_for_area")
            # Activity in bedroom: sleep + dressing + other=================================
            # General
            count_device_status_on_for_area(DFDeviceMap, 'powerStatus', 'bedroom', 'bedroom')
            count_device_status_on_for_area(DFDeviceMap, 'sensedPresence', 'bedroom', 'bedroom')            
            # Sleep
            count_device_status_on_for_area(DFDeviceMap, 'sensedPresence', 'sleep', 'bedroom')            
            # Dressing
            count_device_status_on_for_area(DFDeviceMap, 'sensedPresence', 'wardrobe', 'bedroom')
            count_device_status_on_for_area(DFDeviceMap, 'doorWindowSensor.opneningDetection', 'wardrobe', 'bedroom')
 
            #Activity in bathroom===============================
            logger.info("contador de dispositivo bathroom. count_device_status_on_for_area")
            count_device_status_on_for_area(DFDeviceMap, 'powerStatus', 'bathroom', 'bathroom')
            count_device_status_on_for_area(DFDeviceMap, 'sensedPresence', 'bathroom', 'bathroom')
            count_device_status_on_for_area(DFDeviceMap, 'sensedPresence', 'toilet', 'bathroom')
            count_device_status_on_for_area(DFDeviceMap, 'sensedPresence', 'washbasin', 'bathroom')            
                       
            #Activity in livingroom===============================
            logger.info("contador de dispositivo livingroom. count_device_status_on_for_area")
            count_device_status_on_for_area(DFDeviceMap, 'powerStatus', 'livingroom', 'livingroom')
            count_device_status_on_for_area(DFDeviceMap, 'sensedPresence', 'livingroom', 'livingroom')
            count_device_status_on_for_area(DFDeviceMap, 'sensedPresence', 'armchair', 'livingroom')
            
            #Activity in hallway===============================
            logger.info("contador de dispositivo hallway. count_device_status_on_for_area")
            count_device_status_on_for_area(DFDeviceMap, 'sensedPresence', 'hallway', 'hallway')
            count_device_status_on_for_area(DFDeviceMap, 'powerStatus', 'hallway', 'hallway') 
            count_device_status_on_for_area(DFDeviceMap, 'flood.alarm', 'hallway', 'hallway')

            #Activity in kitchen===============================
            logger.info("contador de dispositivo kitchen. count_device_status_on_for_area")
            count_device_value_on_for_area(DFDeviceMap, 'thermometer.currentTemperature','200' , 'kitchen', 'kitchen')
            count_device_value_on_for_area(DFDeviceMap, 'carbonDioxydeSensor.currentConcentration','0' , 'kitchen', 'kitchen')
            count_device_value_on_for_area(DFDeviceMap, 'carbonMonoxydeSensor.currentConcentration','0' , 'kitchen', 'kitchen')
            count_device_status_on_for_area(DFDeviceMap, 'powerStatus', 'kitchen', 'kitchen')
            count_device_status_on_for_area(DFDeviceMap, 'sensedPresence', 'kitchen', 'kitchen')
            count_device_status_on_for_area(DFDeviceMap, 'sensedPresence', 'chair_kitchen', 'kitchen')



        except BaseException:
            logger.error("=====error==== %s =========" % str(time))
           

    DFDeviceMap = rdd.foreachRDD(process)
    ssc.start()
    time.sleep(6)
    ssc.stop(stopSparkContext=True, stopGraceFully=True)

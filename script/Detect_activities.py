# -*- coding: utf-8 -*-
"""
Created on Mon July 01 23:11:12 2022

@author: Irache Garamendi
"""
import datetime
import time
import logging

from pyspark import SparkConf, SparkContext
from pyspark.rdd import RDD
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import col, lit, create_map,explode,map_keys


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

def createLogger():
    '''
    Initial configuration trace. Create log file in path

    Returns
    -------
    logger : TYPE
        DESCRIPTION.

    '''
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
    return logger    
    pass

if __name__ == "__main__":

    # Spark
    sc = SparkContext(appName="Daylies Activities DigitalDataiCASA")
    ssc = StreamingContext(sc, 60)

    # Crear stream de datos de entrada a través de HDFS
    dstreamimput = ssc.textFileStream("hdfs://localhost:9000/imput")

    # Separar cada línea del fichero
    rdd = dstreamimput.map(lambda x: x.split('\n'))
    logger = createLogger()
    logger.info("Previo process: DataiCasa")
    
    # Convertir RDDs de datos de entrada DStream a DataFrame ,contar actividad, detectar actividades
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

            
            def createDataFrame(rowRdd: RDD[str]):
                '''
                Transformación del RDD a dataframe

                Parameters
                ----------
                rowRdd : RDD[str]
                    DESCRIPTION.

                Returns
                -------
                digitalDataDataFrame : TYPE
                    DESCRIPTION.

                '''
                # Crear dataFrame
                digitalDataDataFrame = spark.createDataFrame(rowRdd)
                logger.info("iCasaDataFrame schema")
                logger.info(digitalDataDataFrame.printSchema())
                digitalDataDataFrame.printSchema()
                digitalDataDataFrame.show(1)
                return digitalDataDataFrame
                pass
            
            def convertColToMap():
                '''
                Convertir a map columnas de un dataframe

                Returns
                -------
                DFDeviceMap : TYPE
                    DESCRIPTION.

                '''
                # Convertir columnas a Map
                DFDeviceMap = digitalDataDataFrame.withColumn("propertiesMap", create_map(
                    lit("propertyDevice"), col("propertyDevice"),
                    lit("valuePropertyDevice"), col("valuePropertyDevice")
                )).drop("propertyDevice", "valuePropertyDevice")

                DFDeviceMap.printSchema()
                DFDeviceMap.first()
                DFDeviceMap = DFDeviceMap.sort(col("dateTime"),col("area"))
                DFDeviceMap.show(truncate=False)
                return DFDeviceMap
                pass

            def countDeviceStatusBedroom():
                '''
                A partir de un data frame con datos de dispositivos digitales se recuenta la actividad \
                    en la habitación. se escribe la información en un fichero: \
                        /resultados/Activity_bedroom_sleep_basic_house_one_day_dataset.txt
                        

                Returns
                -------
                None.

                '''
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
                
                
                result_activity_general_bedroom = df_bedroom_powerStatus.count() + df_bedroom_sensedPresence.count()
                result_activity_bedroom_sleep = df_bedroom_sensedPresence_sleep.count()
                result_activity_bedroom_all_dressing = df_bedroom_sensedPresence_dresssing.count() + df_bedroom_sensedPresence_wardrobe.count()
                
                # Guardar en fichero la información
                f = open("%s/resultados/Activity_bedroom_sleep_basic_house_one_day_dataset.txt" % workPath, "a")
                f.write('[%s] - Activity in bedroom light + presence sensor: %i \n' % (d, result_activity_general_bedroom))
                f.write('[%s] - Activity in bedroom presence sensor bed: %i \n' % (d, result_activity_bedroom_sleep))       
                f.write('[%s] - Activity in bedroom presence sensor wardrobe: %i \n' % (d, result_activity_bedroom_all_dressing))            
                f.close()

                pass
           
            def countDeviceStatusBathroom():
                '''

                A partir de un data frame con datos de dispositivos digitales se recuenta la actividad \
                    en el baño. se escribe la información en un fichero: \
                        /resultados/Activity_bathroom_basic_house_one_day_dataset.txt
                       
                Returns
                -------
                None.

                '''
                #Activity in bathroom===============================
                df_bathroom_powerStatus = DFDeviceMap.filter((DFDeviceMap.propertiesMap.getItem("propertyDevice") == "powerStatus") \
                                                             & (DFDeviceMap.propertiesMap.getItem("valuePropertyDevice") == "on") \
                                                             & (DFDeviceMap["area"] == "bathroom"))
                
                df_bathroom_sensedPresence = DFDeviceMap.filter((DFDeviceMap.propertiesMap.getItem("propertyDevice") == "sensedPresence") \
                                                                & (DFDeviceMap.propertiesMap.getItem("valuePropertyDevice") == "on") \
                                                                & (DFDeviceMap["area"] == "bathroom"))
                                
                df_bathroom_sensedPresence_toilet = DFDeviceMap.filter((DFDeviceMap.propertiesMap.getItem("propertyDevice") == "sensedPresence") \
                                                                       & (DFDeviceMap.propertiesMap.getItem("valuePropertyDevice") == "on") \
                                                                       & (DFDeviceMap["area"] == "toilet"))
                
                df_bathroom_sensedPresence_washbasin = DFDeviceMap.filter((DFDeviceMap.propertiesMap.getItem("propertyDevice") == "sensedPresence") \
                                                                          & (DFDeviceMap.propertiesMap.getItem("valuePropertyDevice") == "on") \
                                                                          & (DFDeviceMap["area"] == "washbasin"))
                
                result_activity_bathroom = df_bathroom_powerStatus.count() + df_bathroom_sensedPresence.count()
                f = open("%s/resultados/Activity_bathroom_basic_house_one_day_dataset.txt" % workPath, "a")
                f.write('[%s] - Activity in bathroom light + presence sensor: %i \n' % (d, result_activity_bathroom))
                f.write('[%s] - Activity in bathroom presence sensor washbasin: %i \n' % (d, df_bathroom_sensedPresence_washbasin.count()))
                f.write('[%s] - Activity in bathroom presence sensor toilet: %i \n' % (d, df_bathroom_sensedPresence_toilet.count()))
                f.close()  

                pass
                       
            def countDeviceStatusKitchen():
                '''
                A partir de un data frame con datos de dispositivos digitales se recuenta la actividad \
                    en la cocina. se escribe la información en un fichero: \
                            /resultados/Activity_kitchen_basic_house_one_day_dataset.txt
                Returns
                -------
                None.

                '''
                #Activity kitchen===============================
                #cooking
                df_kitchen_powerStatus_cooking = DFDeviceMap.filter((DFDeviceMap.propertiesMap.getItem("propertyDevice") == "powerStatus") \
                                                                    & (DFDeviceMap.propertiesMap.getItem("valuePropertyDevice") == "on") \
                                                                    & (DFDeviceMap["area"] == "kitchen"))
                                
                df_kitchen_sensedPresence_cooking = DFDeviceMap.filter((DFDeviceMap.propertiesMap.getItem("propertyDevice") == "sensedPresence") \
                                                                       & (DFDeviceMap.propertiesMap.getItem("valuePropertyDevice") == "on") \
                                                                       & (DFDeviceMap["area"] == "kitchen"))
                
                #Eating
                df_kitchen_sensedPresence_eating = DFDeviceMap.filter((DFDeviceMap.propertiesMap.getItem("propertyDevice") == "sensedPresence") \
                                                                      & (DFDeviceMap.propertiesMap.getItem("valuePropertyDevice") == "on") \
                                                                      & (DFDeviceMap["area"] == "chair_kitchen"))
                                
                result_activity_cooking = df_kitchen_powerStatus_cooking.count() + df_kitchen_sensedPresence_cooking.count()
                result_activity_eating = df_kitchen_sensedPresence_eating.count()
                
                f = open("%s/resultados/Activity_kitchen_basic_house_one_day_dataset.txt" % workPath, "a")
                f.write('[%s] - Activity in kitchen light + presence sensor: %i \n' % (d,  result_activity_cooking ))            
                f.write('[%s] - Activity in kitchen presesensor eating: %i \n' % (d, result_activity_eating))
                f.close()   

                pass
            
            def countDeviceStatusLivingroom():
                '''
                A partir de un data frame con datos de dispositivos digitales se recuenta la actividad \
                    en la sala de estar. se escribe la información en un fichero: \
                         /resultados/Activity_livingroom_SetupHouseWithLights_test_one_day.txt
                Returns
                -------
                None.

                '''
                #Activity in livingroom ===============================
                df_livingroom_powerStatus = DFDeviceMap.filter((DFDeviceMap.propertiesMap.getItem("propertyDevice") == "powerStatus") \
                                                               & (DFDeviceMap.propertiesMap.getItem("valuePropertyDevice") == "on") \
                                                               & (DFDeviceMap["area"] == "livingroom"))

                df_livingroom_sensedPresence = DFDeviceMap.filter((DFDeviceMap.propertiesMap.getItem("propertyDevice") == "sensedPresence") \
                                                                  & (DFDeviceMap.propertiesMap.getItem("valuePropertyDevice") == "on") \
                                                                  & (DFDeviceMap["area"] == "livingroom"))
                
                df_livingroom_sensedPresence_armchair = DFDeviceMap.filter((DFDeviceMap.propertiesMap.getItem("propertyDevice") == "sensedPresence") \
                                                                           & (DFDeviceMap.propertiesMap.getItem("valuePropertyDevice") == "on") \
                                                                           & (DFDeviceMap["area"] == "armchair"))
                
                result_activity_livingroom = df_livingroom_powerStatus.count() + df_livingroom_sensedPresence.count()+df_livingroom_sensedPresence_armchair.count()
                f = open("%s/resultados/Activity_livingroom_SetupHouseWithLights_test_one_day.txt" % workPath, "a")
                f.write('[%s] - Activity in livingroom light + presence sensor: %i \n' % (d, result_activity_livingroom))
                f.close()    

                pass
            
            def detectActivityDailyMovement():           
                '''
                A partir de datos de dispositivos digitales colocados en distintas zonas de una casa
                se detectan desplazamientos dentro de la misma
                El resultado se escibe en un fichero.
                
                Returns
                -------
                None.

                '''
                DFDeviceMapSortCollet = DFDeviceMap.collect() 
                
                f = open("%s/resultados/Activity_movement.txt" % workPath, "a")                   
                contadorRow = 0
                
                for rowDFDeviceMapSortCollet in DFDeviceMapSortCollet:
                    if ((rowDFDeviceMapSortCollet['propertiesMap']['propertyDevice'] == 'sensedPresence') \
                        and (rowDFDeviceMapSortCollet['propertiesMap']['valuePropertyDevice']=='on')):
                        for rowDFDeviceMapSortColletMovimiento in DFDeviceMapSortCollet:
                            if ((rowDFDeviceMapSortColletMovimiento['propertiesMap']['propertyDevice'] == 'sensedPresence') \
                                and (rowDFDeviceMapSortColletMovimiento['propertiesMap']['valuePropertyDevice']=='on') \
                                and (rowDFDeviceMapSortColletMovimiento['area'] != rowDFDeviceMapSortCollet['area']) \
                                and (rowDFDeviceMapSortColletMovimiento['dateTime'] > rowDFDeviceMapSortCollet['dateTime'])):
                                f.write('Se detecta movimiento, cambios de zona desde zona 1: %s a zona 2: %s \n' %(str(rowDFDeviceMapSortCollet['area']), str(rowDFDeviceMapSortColletMovimiento['area'])))
                                break            
                    contadorRow += 1
                f.close() 
                pass
            
            def detectActivityDailyToilet():           
                '''
                A partir de datos de dispositivos digitales colocados en el toilet
                se detectan usos del mismo.
                El resultado se escibe en un fichero.

                Returns
                -------
                None.

                '''
                DFDeviceMapSortCollet = DFDeviceMap.collect()
               
                f = open("%s/resultados/Activity_toilet.txt" % workPath, "a")
                   
                contadorRow = 0
                for rowDFDeviceMapSortCollet in DFDeviceMapSortCollet:
                    if ((rowDFDeviceMapSortCollet['area'] == 'toilet') \
                        and (rowDFDeviceMapSortCollet['propertiesMap']['propertyDevice'] == 'sensedPresence') \
                        and (rowDFDeviceMapSortCollet['propertiesMap']['valuePropertyDevice'] == 'on')):
                        for rowDFDeviceMapSortColletToilet in DFDeviceMapSortCollet:
                            if ((rowDFDeviceMapSortColletToilet['area'] == 'toilet') \
                                and (rowDFDeviceMapSortColletToilet['propertiesMap']['propertyDevice'] == 'sensedPresence') \
                                and (rowDFDeviceMapSortColletToilet['propertiesMap']['valuePropertyDevice']=='off') \
                                and (rowDFDeviceMapSortColletToilet['dateTime'] > rowDFDeviceMapSortCollet['dateTime'])): 
                                f.write('Se detecta el uso del baño: %s \n' % (str(rowDFDeviceMapSortColletToilet))) 
                                tiempoEntrada = str(rowDFDeviceMapSortCollet['dateTime'])
                                tiempoSalida = str(rowDFDeviceMapSortColletToilet['dateTime'])
                                f.write('Tiempo entrada: %s , tiempo salida: %s \n' % (tiempoEntrada,tiempoSalida))
                                break                 
                    contadorRow += 1
                f.close() 
                pass
            
            def detectActivityDailyWashbasin():           
                '''
                A partir de datos de dispositivos digitales colocados en el lavabo
                se detectan usos del mismo.
                El resultado se escibe en un fichero.

                Returns
                -------
                None.

                '''
                DFDeviceMapSortCollet = DFDeviceMap.collect()
               
                f = open("%s/resultados/Activity_washbasin.txt" % workPath, "a")
                   
                contadorRow = 0
                for rowDFDeviceMapSortCollet in DFDeviceMapSortCollet:
                    if ((rowDFDeviceMapSortCollet['area'] == 'washbasin') \
                        and (rowDFDeviceMapSortCollet['propertiesMap']['propertyDevice'] == 'sensedPresence') \
                        and (rowDFDeviceMapSortCollet['propertiesMap']['valuePropertyDevice'] == 'on')):
                        for rowDFDeviceMapSortColletWashbasin in DFDeviceMapSortCollet:
                            if ((rowDFDeviceMapSortColletWashbasin['area'] == 'washbasin') \
                                and (rowDFDeviceMapSortColletWashbasin['propertiesMap']['propertyDevice'] == 'sensedPresence') \
                                and (rowDFDeviceMapSortColletWashbasin['propertiesMap']['valuePropertyDevice']=='off') \
                                and (rowDFDeviceMapSortColletWashbasin['dateTime'] > rowDFDeviceMapSortCollet['dateTime'])): 
                                f.write('Se detecta el uso del lavabo: %s \n' % (str(rowDFDeviceMapSortColletWashbasin))) 
                                tiempoEntrada = str(rowDFDeviceMapSortCollet['dateTime'])
                                tiempoSalida = str(rowDFDeviceMapSortColletWashbasin['dateTime'])
                                f.write('Tiempo entrada: %s , tiempo salida: %s \n' % (tiempoEntrada,tiempoSalida))
                                break                 
                    contadorRow += 1
                f.close() 
                pass
            
            def detectActivityDailykitchen():           
                '''
                A partir de datos de dispositivos digitales colocados en la cocina
                se detectan usos de la misma por ejemplo para cocinar.
                El resultado se escibe en un fichero.

                Returns
                -------
                None.

                '''
                DFDeviceMapSortCollet = DFDeviceMap.collect()
               
                f = open("%s/resultados/Activity_kitchen.txt" % workPath, "a")
                   
                contadorRow = 0
                for rowDFDeviceMapSortCollet in DFDeviceMapSortCollet:
                    if ((rowDFDeviceMapSortCollet['area'] == 'kitchen') \
                        and (rowDFDeviceMapSortCollet['propertiesMap']['propertyDevice'] == 'sensedPresence') \
                        and (rowDFDeviceMapSortCollet['propertiesMap']['valuePropertyDevice'] == 'on')):
                        for rowDFDeviceMapSortColletkitchen in DFDeviceMapSortCollet:
                            if ((rowDFDeviceMapSortColletkitchen['area'] == 'kitchen') \
                                and (rowDFDeviceMapSortColletkitchen['propertiesMap']['propertyDevice'] == 'sensedPresence') \
                                and (rowDFDeviceMapSortColletkitchen['propertiesMap']['valuePropertyDevice']=='off') \
                                and (rowDFDeviceMapSortColletkitchen['dateTime'] > rowDFDeviceMapSortCollet['dateTime'])): 
                                f.write('Se detecta el uso de la cocina: %s \n' % (str(rowDFDeviceMapSortColletkitchen))) 
                                tiempoEntrada = str(rowDFDeviceMapSortCollet['dateTime'])
                                tiempoSalida = str(rowDFDeviceMapSortColletkitchen['dateTime'])
                                f.write('Tiempo entrada: %s , tiempo salida: %s \n' % (tiempoEntrada,tiempoSalida))
                                break                 
                    contadorRow += 1
                f.close() 
                pass
            
            def detectActivityDailyEating():           
                '''
                A partir de datos de dispositivos digitales colocados en la silla de la cocina
                se detectan usos de la misma, por ejemplo para comer.
                El resultado se escibe en un fichero.

                Returns
                -------
                None.

                '''
                DFDeviceMapSortCollet = DFDeviceMap.collect()
               
                f = open("%s/resultados/Activity_eating.txt.txt" % workPath, "a")
                   
                contadorRow = 0
                for rowDFDeviceMapSortCollet in DFDeviceMapSortCollet:
                    if ((rowDFDeviceMapSortCollet['area'] == 'chair_kitchen') \
                        and (rowDFDeviceMapSortCollet['propertiesMap']['propertyDevice'] == 'sensedPresence') \
                        and (rowDFDeviceMapSortCollet['propertiesMap']['valuePropertyDevice'] == 'on')):
                        for rowDFDeviceMapSortColleteating in DFDeviceMapSortCollet:
                            if ((rowDFDeviceMapSortColleteating['area'] == 'chair_kitchen') \
                                and (rowDFDeviceMapSortColleteating['propertiesMap']['propertyDevice'] == 'sensedPresence') \
                                and (rowDFDeviceMapSortColleteating['propertiesMap']['valuePropertyDevice']=='off') \
                                and (rowDFDeviceMapSortColleteating['dateTime'] > rowDFDeviceMapSortCollet['dateTime'])): 
                                f.write('Se detecta el uso de la mesa de la cocina: %s \n' % (str(rowDFDeviceMapSortColleteating))) 
                                tiempoEntrada = str(rowDFDeviceMapSortCollet['dateTime'])
                                tiempoSalida = str(rowDFDeviceMapSortColleteating['dateTime'])
                                f.write('Tiempo entrada: %s , tiempo salida: %s \n' % (tiempoEntrada,tiempoSalida))
                                break                 
                    contadorRow += 1
                f.close() 
                pass
            
            def detectActivityDailyLivingroom():           
                '''
                A partir de datos de dispositivos digitales colocados en la sala de estar
                se detectan usos de la misma.
                El resultado se escibe en un fichero.

                Returns
                -------
                None.

                '''
                DFDeviceMapSortCollet = DFDeviceMap.collect()
               
                f = open("%s/resultados/Activity_Livingroom.txt" % workPath, "a")
                   
                contadorRow = 0
                for rowDFDeviceMapSortCollet in DFDeviceMapSortCollet:
                    if ((rowDFDeviceMapSortCollet['area'] == 'armchair') \
                        and (rowDFDeviceMapSortCollet['propertiesMap']['propertyDevice'] == 'sensedPresence') \
                        and (rowDFDeviceMapSortCollet['propertiesMap']['valuePropertyDevice'] == 'on')):
                        for rowDFDeviceMapSortColletLivingroom in DFDeviceMapSortCollet:
                            if ((rowDFDeviceMapSortColletLivingroom['area'] == 'armchair') \
                                and (rowDFDeviceMapSortColletLivingroom['propertiesMap']['propertyDevice'] == 'sensedPresence') \
                                and (rowDFDeviceMapSortColletLivingroom['propertiesMap']['valuePropertyDevice']=='off') \
                                and (rowDFDeviceMapSortColletLivingroom['dateTime'] > rowDFDeviceMapSortCollet['dateTime'])): 
                                f.write('Se detecta el uso de la sala de estar: %s \n' % (str(rowDFDeviceMapSortColletLivingroom))) 
                                tiempoEntrada = str(rowDFDeviceMapSortCollet['dateTime'])
                                tiempoSalida = str(rowDFDeviceMapSortColletLivingroom['dateTime'])
                                f.write('Tiempo entrada: %s , tiempo salida: %s \n' % (tiempoEntrada,tiempoSalida))
                                break                 
                    contadorRow += 1
                f.close() 
                pass
            
            digitalDataDataFrame = createDataFrame(rowRdd)
            DFDeviceMap = convertColToMap()
            countDeviceStatusBedroom()
            countDeviceStatusBathroom()
            countDeviceStatusKitchen()
            countDeviceStatusLivingroom()
            detectActivityDailyMovement()
            detectActivityDailyToilet()
            detectActivityDailyWashbasin()
            detectActivityDailykitchen()
            detectActivityDailyEating()
            
        except BaseException:
            logger.error("=====error==== %s =========" % str(time))
            
    DFDeviceMap = rdd.foreachRDD(process)
    ssc.start()
    time.sleep(6)
    ssc.stop(stopSparkContext=True, stopGraceFully=True)
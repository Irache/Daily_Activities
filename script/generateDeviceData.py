# -*- coding: utf-8 -*-
"""
Created on Fri Jul 29 19:12:00 2022

@author: Irache Garamendi Bragado
"""

import os
import sys
from xml.dom import minidom
import datetime
import time
import pathlib

# Script lee los ficheros que flume ha dejado en un directorio
# Sólo trabaja con los ficheros de tamaño distinto de cero, ya que flume crea más ficheros
# Elimina los ficheros de tamaño cero
# Renombra aquellos ficheros que hayan sido tratados
# Recibe dos parametros de entrada path y path_spark
# path: parametro de entrada, debe ser el directorio de flume
# path_spark: parametro de entrada donde se dejan los ficheros generados csv
path = sys.argv[1]
path_spark = sys.argv[2]
# Genera la lista de todos los ficheros del directorio path
 
fun = lambda x : os.path.isfile(os.path.join(path,x))
files_list = filter(fun, os.listdir(path))

# Crea la lista de ficheros del directorio path junto con su tamaño
size_of_file = [
    (f,os.stat(os.path.join(path, f)).st_size)
    for f in files_list
]

# Iterar a traves de la lista de ficheros para crear el csv a partir solo de aquellos ficheros que tienen tamaño

for f,s in size_of_file:
    try:
        pathfile = pathlib.Path(f)
        extension = ''.join(pathfile.suffixes)

        if "COMPLETED" in extension:
        #No hacer nada   
            nada = extension
        else:
            if round(s/(1024*1024),3) != 0.0:
                #print("{} : {}mb".format(f, round(s/(1024*1024),3)))
                fileflumevirtualhome = path+"//"+str(f)
                salida = path_spark +"//"+str(f)+".csv"        
                entrada = fileflumevirtualhome
    
                # bloque para leer el fichero de entrada y generar el csv de salida
                doc = minidom.parse(entrada)
                # Los ficheros de entrada son xml por eso se pueden leer sus elementos
                behavior = doc.getElementsByTagName("behavior")[0]
                start_time = datetime.datetime.strptime(behavior.getAttribute("startdate"), '%d/%m/%Y-%H:%M:%S')
                datedevice = start_time
                
                # fileoutput es el fichero de salida
                fileoutput = open(salida, "a")
                
                devices = doc.getElementsByTagName("set-device-property")
                zones = doc.getElementsByTagName("move-device-zone")
                times = doc.getElementsByTagName("delay")
                i=0
                escribir_lista_horas = []
                for time in times:
                    if time.getAttribute("unit") == "h":
                        datedelay = datetime.timedelta(hours=int(time.getAttribute("value")),minutes=00,seconds=000)
                    else:
                        datedelay = datetime.timedelta(hours=00,minutes=int(time.getAttribute("value")),seconds=000)  
                    datedevice = datedelay + datedevice    
                    escribir_lista_horas.append(str(datedevice))
                for device in devices:        
                    sid = device.getAttribute("deviceId")
                    porperty = device.getAttribute("property")
                    value = device.getAttribute("value") 
                    fileoutput.write(escribir_lista_horas[i] + ',' + sid + ',' + porperty + ',' + value + ',')
                    for zone in zones:
                        devicezone = zone.getAttribute("deviceId")
                        zone = zone.getAttribute("zoneId")
                        if sid == devicezone:            
                            fileoutput.write(zone + '\n')
                            i=i+1
                datedevice = datedevice
                fileoutput.close() 
                
                #bloque para renombrar los ficheros ya tratados
                
                nombre_nuevo = fileflumevirtualhome + ".COMPLETED"
                os.rename(fileflumevirtualhome, nombre_nuevo)
    
            else:
                try:
                    os.remove(path+"//"+str(f))
                except:
                    raise
    except:
        raise
    

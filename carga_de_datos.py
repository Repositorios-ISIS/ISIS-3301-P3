# webdriver ayuda a realizar acciones de navegación en el navegador
from selenium import webdriver
# time ayuda a poder detener el script un tiempo determinado
import time
# Para saber donde estoy :v
import os
#Librerias para trabajar los datos
import pandas as pd
import numpy as np
# Para trabajar con archivos zip
from zipfile import ZipFile

# Función para descargar los archivos de los municipios
def descargar_archivos(lista):

    """
    Configuración opciones del webdriver
    """

    # Ayuda a establecer las preferencias del navegador chrome
    options = webdriver.ChromeOptions()
    prefs = {'download.default_directory': os.getcwd() + '\\data_source'}
    #Permite añadir nuestras preferencias al driver
    options.add_experimental_option("prefs",prefs)

    """
    Configuración webdriver
    """
    # Ruta del driver
    PATH = os.getcwd() + '\\chromedriver\\chromedriver.exe'
    # Creamos el driver
    driver = webdriver.Chrome(executable_path=PATH, options = options)

    """
    Ahora si la descarga
    """

    for muni in lista:
        file_name = f'./data_source/TerriData{muni}f.xlsx.zip'
        # Para cada municipio, si este zip no esta en "./data_source/", se descarga
        if not os.path.isfile(file_name):
            print('descargando municipio: ' + str(muni))
            try:
                # Buscamos el objeto
                driver.get(f'https://terridata.dnp.gov.co/index-app.html#/perfiles/{muni}')
                time.sleep(2)
                downloadxlsx = driver.find_element_by_id('btnExcel')
                downloadxlsx.click()
                time.sleep(8)
            except Exception as e:
                print(str(e))
            # Esperamos 30 segundos porque no quiero que la página banee mi ip
            time.sleep(3)
        print(f'Municipio {muni} descargado.')

    #Cerramos el webdriver
    driver.quit()

# Arregla los tipos del dataframe
def arreglar_tipos(df:pd.DataFrame, types:dict) -> None:
    for value in types:
        try:
            df[value] = df[value].astype(types[value])
        except Exception as e:
            print(f'error in value {value}',str(e))

def get_main_dataframe(name):
    # Dataframe central
    df_main = pd.read_csv(name,sep=',',index_col=False,low_memory=False)

    # Limpieza
    df_main = df_main.drop(df_main[df_main['codigo_depto']=='TOTAL'].index,errors='ignore')
    df_main = df_main.drop(df_main[df_main['nombre_depto']=='TOTAL'].index,errors='ignore')
    df_main = df_main.drop(df_main[df_main['nombre_muni']=='TOTAL'].index,errors='ignore')
    df_main = df_main.drop(df_main[df_main['codigo_muni']=='TOTAL'].index,errors='ignore')
    df_main = df_main.drop(df_main[df_main['codigo_evento']=='TOTAL'].index,errors='ignore')
    df_main = df_main.drop(df_main[df_main['evento']=='TOTAL'].index,errors='ignore')
    df_main = df_main.drop(df_main[df_main['edad']=='TOTAL'].index,errors='ignore')
    df_main = df_main.drop(df_main[df_main['sexo']=='TOTAL'].index,errors='ignore')

    # Lo de los extranjeros
    df_main['codigo_depto'] = df_main['codigo_depto'].apply(lambda x : 100 if x == 'Extranjeros' else x)

    # Quitamos muertes en el extranjero (columna codigo_depto con el valor 100)
    df_main = df_main.drop(df_main[df_main['codigo_depto']==100].index)
    # Quitamos sexo indeterminado (columna sexo con el valor "Indeterminado")
    df_main = df_main.drop(df_main[df_main['sexo']=='Indeterminado'].index)
    # Quitamos muertes de edad desconocida (columna edad con valor "Edad Desconocida")
    df_main = df_main.drop(df_main[df_main['edad']=='Edad desconocida'].index)

    """
    En la columna edad quitaremos los valores correspondientes a "De 65 y más" dado que en esa columna a
    la vez hay rangos como "De 65-84 años", "De 85-99 años", "De 100 y más" y al ser dos versiones de la
    misma información (aparentemente), decidimos quedarnos con los otros rangos en espera de que nos den
    más info.
    """
    df_main = df_main.drop(df_main[df_main['edad']=='De 65 y más'].index)

    # Arreglamos los tipos
    value_types = {'anio':'int64','codigo_depto':'string','nombre_depto':'string',
                   'nombre_muni':'string','codigo_muni':'string','codigo_evento':'int64',
                   'evento':'string','sexo':'string','edad':'string','num_casos':'int64'
                  }
    arreglar_tipos(df_main,value_types)

    # Hay que arreglar los rangos de las edades
    rangos = {'Menor 1 año':'0-4 años','De 1-4 años':'0-4 años','De 5-14 años':'5-14 años',
              'De 15-44 años':'15-44 años','De 45-64 años':'45-64 años','De 65-84 años':'65+ años',
              'De 85-99 años':'65+ años','De 100 y más':'65+ años'
             }

    df_main['edad'] = df_main['edad'].apply(lambda x : rangos[x])

    #Los valores que nos hayan quedado iguales deben ser sumados
    group_by_parameters = ['anio','codigo_depto','nombre_depto','nombre_muni','codigo_muni',
                           'codigo_evento','evento','edad','sexo']
    df_main = df_main.groupby(by=group_by_parameters).sum()
    #Para volverlo dataframe denuevo
    df_main = df_main.reset_index()

    # Imprimo
    #df_main.head()

    return df_main

# Consigo el df principal
df_main = get_main_dataframe('data_source/data_2018_2020.csv')

# Aqui saco solo el Caquetá por el test
#df_main = df_main[df_main["nombre_depto"]=="Caquetá"]

# Me quedo con los códigos
muni_list = df_main["codigo_muni"].unique()

#Hora de descargar los archivos
descargar_archivos(muni_list)

df_ready = None
first_time = False

for muni in muni_list:
    filename = f'./data_source/TerriData{muni}f.xlsx'
    try:
        zip = ZipFile((filename+'.zip'), 'r')
        zip.printdir()
        zip.extractall('./data_source/')
        
        # Cargo el archivo
        value_types = {'Código Departamento':'string','Código Entidad':'string'}
        temp = pd.read_excel(filename,dtype=value_types)

        # Me quedo solo con lo que me interesa
        temp = temp[(temp["Dimensión"] == "Demografía y población")
                    &
                    (
                        (temp["Subcategoría"] == "Población de hombres")
                        |
                        (temp["Subcategoría"] == "Población de mujeres")
                    )
                   ]

        # Solo conservo las columnas que quiero
        temp = temp.drop(columns=['Dimensión','Subcategoría','Dato Cualitativo','Mes','Fuente'],errors='ignore')

        # Arreglo los nombres de las columnas
        temp.columns = ['codigo_depto','nombre_depto','codigo_muni','nombre_muni','edad','población','anio','sexo']

        # Toca cambiar los '.' por ',' en la columna población  y visceversa
        temp['población'] = temp['población'].apply(lambda x : x.replace('.',''))
        temp['población'] = temp['población'].apply(lambda x : x.replace(',','.'))
        temp['población'] = temp['población'].astype('float64')

        # Arreglamos los tipos
        value_types = {'codigo_depto':'string','nombre_depto':'string','codigo_muni':'string',
                       'nombre_muni':'string','edad':'string','población':'int64',
                       'anio':'int64','sexo':'string'
                      }

        arreglar_tipos(temp,value_types)

        # Hay que arreglar los rangos de las edades
        rangos_temp = {'Población de hombres de 00-04':'0-4 años','Población de hombres de  05-09':'5-14 años',
                     'Población de hombres de  10-14':'5-14 años','Población de hombres de  15-19':'15-44 años',
                     'Población de hombres de  20-24':'15-44 años','Población de hombres de  25-29':'15-44 años',
                     'Población de hombres de  30-34':'15-44 años','Población de hombres de  35-39':'15-44 años',
                     'Población de hombres de  40-44':'15-44 años','Población de hombres de  45-49':'45-64 años',
                     'Población de hombres de  50-54':'45-64 años','Población de hombres de  55-59':'45-64 años',
                     'Población de hombres de  60-64':'45-64 años','Población de hombres de  65-69':'65+ años',
                     'Población de hombres de  70-74':'65+ años','Población de hombres de  75-79':'65+ años',
                     'Población de hombres de 80 o más':'65+ años','Población de mujeres de 00-04':'0-4 años',
                     'Población de mujeres de  05-09':'5-14 años','Población de mujeres de  10-14':'5-14 años',
                     'Población de mujeres de  15-19':'15-44 años','Población de mujeres de  20-24':'15-44 años',
                     'Población de mujeres de  25-29':'15-44 años','Población de mujeres de  30-34':'15-44 años',
                     'Población de mujeres de  35-39':'15-44 años','Población de mujeres de  40-44':'15-44 años',
                     'Población de mujeres de  45-49':'45-64 años','Población de mujeres de  50-54':'45-64 años',
                     'Población de mujeres de  55-59':'45-64 años','Población de mujeres de  60-64':'45-64 años',
                     'Población de mujeres de  65-69':'65+ años','Población de mujeres de  70-74':'65+ años',
                     'Población de mujeres de  75-79':'65+ años','Población de mujeres de 80 o más':'65+ años'
                    }

        temp['edad'] = temp['edad'].apply(lambda x : rangos_temp[x])

        #Los valores que nos hayan quedado iguales deben ser sumados
        temp_group_by_parameters_1 = ['codigo_depto','nombre_depto','codigo_muni',
                                      'nombre_muni','edad','anio','sexo'
                                     ]
        temp = temp.groupby(by=temp_group_by_parameters_1).sum()
        #Para volverlo dataframe denuevo
        temp = temp.reset_index()

        # Juntamos con el dataframe original
        temp_group_by_parameters_2 = ['codigo_depto','nombre_depto','codigo_muni',
                                      'nombre_muni','edad','anio','sexo'
                                     ]

        temp_merged = pd.merge(df_main, temp,  on=temp_group_by_parameters_2)
        if first_time:
            df_ready = temp_merged
            first_time = False
        else:
            df_ready = pd.concat([df_ready, temp_merged])

        os.remove(filename)
    except Exception as e:
        print(str(e))
        
df_ready.to_csv('data_generated/data_ready.csv', index = False)

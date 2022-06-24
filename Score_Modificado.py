
### LIBRERIAS
from joblib import load
import numpy as np 
import pandas as pd
import pickle
import time
from smtplib import SMTPException
import smtplib
from xgboost import XGBClassifier
import pyodbc

class Model():
    def execute():
        data = {'Prueba': ['EJECUTANDO']}
        data= DataFrame(data)
        data.to_csv('E:\InterCambioOperacion\AUTOS\SOAT_FRAUDEV2\model1.csv', index=None)

        conn=pyodbc.connect('Driver={SQL Server};'
                   'Server=DC1PPDSQL1'
                   'Database=SIT_MINERIA;'
                   'Trusted_Connection=yes;')

        cursor = conn.cursor()
        TodaysDate = time.strftime("%Y%m%d")
        
        cursor.execute("""SELECT TOP(1) VALOR FROM [SIT_MINERIA].[FRAUD].[TBL_MN_PFRAU_ACTCORTE]
                WHERE TIPO='PORCENTAJE'
                ORDER BY fecha_creacion desc""")
        valor_corte = cursor.fetchone()
        valor_corte = valor_corte[0]
        
        cursor.execute("""
        SELECT CASE 
        WHEN (SELECT CASE WHEN FEC_CREACION = CONVERT(VARCHAR(8),GETDATE(),112) THEN 'SI_EXISTE' ELSE 'NO_EXISTE' END VALIDA 
        FROM (SELECT MAX(CONVERT(VARCHAR(8),FEC_CREACION,112)) AS FEC_CREACION FROM FRAUD.TBL_MN_PFRAU_PREDICCIONES_HIS) TD) = 'NO_EXISTE' 
            AND (SELECT COUNT(DISTINCT STR_NOMBRE_PAQUETE) TOT  FROM [SIT_MINERIA].[LOG].[TBL_PRM_ARCHIVOS]  WHERE CONVERT(VARCHAR(8),DTM_FECHA_PROCESO,112) = CONVERT(VARCHAR(8),GETDATE(),112)  AND STR_NOMBRE_PAQUETE = 'RECLAMACIONES' AND STR_INDICADOR_PROCESADO = 1) = 1
            AND (SELECT COUNT(*) FROM [SIT_MINERIA].[FRAUD].[TBL_MN_PFRAU_DATAQUALITY] WHERE FECHA_PROCESO = CONVERT(VARCHAR(10),GETDATE(),120)) > 0 
            THEN 'EJECUTAR' 
            ELSE 'NO_EJECUTAR' 
            END               
        """)
        VALIDA_MODEL = cursor.fetchone()
        VALIDA_MODEL = VALIDA_MODEL[0]
        
        cursor.execute("SELECT COUNT(*) AS TOTAL FROM FRAUD.TBL_MN_PFRAU_RECLAMACION_MODEL_V")
        REGISTROS = cursor.fetchone()
        REGISTROS = REGISTROS[0]
        data = {'Prueba': ['EJECUTANDO']}
        data= DataFrame(data)
        data.to_csv('E:\InterCambioOperacion\AUTOS\SOAT_FRAUDEV2\model2.csv', index=None)        
        if (VALIDA_MODEL == "EJECUTAR" and REGISTROS != 0):
            
            df = pd.read_sql_query("""
            SELECT A.NUMERO_SINIESTRO,CAST(A.NUMERO_RECLAMACION AS VARCHAR) as Reclamacion, CAST(A.NUMERO_POLIZA AS VARCHAR) AS NUMERO_POLIZA, A.NUMERO_FACTURA, A.FECHA_OCURRENCIA,B.FECHA_RECP_ASEGURADORA, CAST(A.CEDULA_ACCIDENTADO AS VARCHAR) AS CEDULA_ACCIDENTADO,
            A.HORA_OCURRENCIA, A.EXISTE_RAF,A.IPS_ESQUEMA, A.IND_FRAUDE,  A.FUENTE_FRAUDE, A.ALERTAS, A.FECHA_ESTADO_ACTUAL,A.FECHA_PROCESO, CAST(A.NIT_RECLAMANTE AS VARCHAR) as 'NIT Reclamante', B.VALOR_COBRADO, 
            B.TIPO_RECLAMACION,COD_MUNICIPIO_OCURRENCIA AS Cod_ciu_Ocurrencia, A.COD_MUNICIPIO_RECLAMANTE AS Cod_ciudad_Reclamante, TIPO_DOCUMENTO_RECLAMANTE AS TIPO_DOC_RECLAMANTE,
            B.GENERO, B.TIPO_VEHICULO AS 'Tipo Vehiculo', B.CONDICION_ACCIDENTADO AS Condicion_accidentado, C.COD_DEPARTAMENTO AS Cod_Depto_Reclamante,D.COD_DEPARTAMENTO AS Cod_Depto_Ocurrencia, A.CASO_SOSPECHOSO
            FROM FRAUD.TBL_MN_PFRAU_RECLAMACION_MODEL_V A
            LEFT JOIN
            (SELECT * FROM SIT_MINERIA.FRAUD.TBL_MN_PFRAU_RECLAMACION) B ON (A.NUMERO_RECLAMACION=B.NUMERO_RECLAMACION AND A.NUMERO_SINIESTRO=B.NUMERO_SINIESTRO AND A.NUMERO_POLIZA=B.NUMERO_POLIZA AND A.NUMERO_FACTURA=B.NUMERO_FACTURA)
            LEFT JOIN
            (SELECT * FROM FRAUD.TBL_MN_PFRAU_DANE
            WHERE ANIO_PROYECCION=2020) C ON C.COD_MUNICIPIO=A.COD_MUNICIPIO_RECLAMANTE
            LEFT JOIN
            (SELECT * FROM FRAUD.TBL_MN_PFRAU_DANE
            WHERE ANIO_PROYECCION=2020) D ON D.COD_MUNICIPIO=B.COD_MUNICIPIO_OCURRENCIA
                                    """,conn)
            df=df[~df['NUMERO_FACTURA'].str.contains("DOBLE")]
            df['OBSERVACION'] = np.where(df['CASO_SOSPECHOSO'] == '', 'NO CUMPLE REGLAS', 'CUMPLE REGLAS PARA ASIGNAR A PREVENCION FRAUDE')
            df['IPS_ESQUEMA'] = np.where(df['IPS_ESQUEMA'] == 'X','SI','NO')
            data = {'Prueba': ['EJECUTANDO']}
            data= DataFrame(data)
            data.to_csv('E:\InterCambioOperacion\AUTOS\SOAT_FRAUDEV2\model3.csv', index=None)            
            bda = df[['Reclamacion','NIT Reclamante', 'Cod_Depto_Ocurrencia',
            'Cod_ciu_Ocurrencia', 'Cod_Depto_Reclamante', 'Cod_ciudad_Reclamante','TIPO_DOC_RECLAMANTE', 
            'GENERO','Tipo Vehiculo', 'Condicion_accidentado']]
            
            bda['Cod_Depto_Ocurrencia']=np.where(bda['Cod_Depto_Ocurrencia']=='11','25',bda['Cod_Depto_Ocurrencia'])
            bda['Cod_Depto_Reclamante']=np.where(bda['Cod_Depto_Reclamante']=='11','25',bda['Cod_Depto_Reclamante'])
            
            bda['Reclamacion']=bda['Reclamacion'].astype(np.int64)
            bda['Cod_Depto_Ocurrencia']=bda['Cod_Depto_Ocurrencia'].astype(np.float64)
            bda['Cod_ciu_Ocurrencia']=bda['Cod_ciu_Ocurrencia'].astype(np.float64)
            bda['Cod_Depto_Reclamante']=bda['Cod_Depto_Reclamante'].astype(np.float64)
            bda['Cod_ciudad_Reclamante']=bda['Cod_ciudad_Reclamante'].astype(np.float64)
            
            bda['ReclamoVSCircula_Dpto'] = bda.apply(lambda x: True if x['Cod_Depto_Ocurrencia'] == x['Cod_Depto_Reclamante'] else False, axis = 1)
            bda['ReclamoVSCircula_Mpio'] = bda.apply(lambda x: True if x['Cod_ciu_Ocurrencia'] == x['Cod_ciudad_Reclamante'] else False, axis = 1)
            
            #bda['TIPO_VEHICULO'] = bda.apply(lambda x: TipoVehiculo.tipoVehiculo(x['Tipo Vehiculo']),axis = 1)
            bda['TIPO_VEHICULO']=np.where(bda['Tipo Vehiculo'].isin(['AUTOS DE NEGOCIOS, TAXIS Y MICROBUSES URBANOS','AUTOS FAMILIARES']),'AUTOS',np.where(bda['Tipo Vehiculo'].isin(['MOTOS']),'MOTOS',np.where(bda['Tipo Vehiculo'].isin(['CAMPEROS O CAMIONETAS']),'CAMPEROS',np.where(bda['Tipo Vehiculo'].isin(['VEHICULOS DE SERVICIO PUBLICO INTERMUNICIPAL']),'INTERMUNICIPAL','OTROS'))))
            
            bda['COND_ACCIDENTADO'] = bda['Condicion_accidentado'].replace('PEATÃ³N','PEATÓN')
            bda['COND_ACCIDENTADO'] = bda['COND_ACCIDENTADO'].replace('PEATÓN','PEATON')
            
            bda['Cod_ciu_Ocurrencia'] = bda['Cod_ciu_Ocurrencia'].astype(int).astype(str)
            bda['Cod_ciudad_Reclamante'] = bda['Cod_ciudad_Reclamante'].astype(int).astype(str)        
#            bda['Cod_Depto_Ocurrencia'] = (bda['Cod_Depto_Ocurrencia'].fillna('-1')).astype(int)
            bda['Cod_Depto_Ocurrencia'] = (1000*(bda['Cod_Depto_Ocurrencia'])).astype(str)
#            bda['Cod_Depto_Ocurrencia'] = bda['Cod_Depto_Ocurrencia'].replace('-1000', np.nan)    
#            bda['Cod_Depto_Reclamante'] = (bda['Cod_Depto_Reclamante'].fillna('-1')).astype(int)
            bda['Cod_Depto_Reclamante'] = (1000*(bda['Cod_Depto_Reclamante'])).astype(str)
#            bda['Cod_Depto_Reclamante'] = bda['Cod_Depto_Reclamante'].replace('-1000', np.nan)
            
            data = {'Prueba': ['EJECUTANDO']}
            data= DataFrame(data)
            data.to_csv('E:\InterCambioOperacion\AUTOS\SOAT_FRAUDEV2\model4.csv', index=None)            

            df_HMO = pd.read_sql_query("SELECT * FROM [SIT_MINERIA].[FRAUD].[TBL_MN_PFRAU_MPIO_OCURRENCIA]",conn)
            df_HMR = pd.read_sql_query("SELECT * FROM [SIT_MINERIA].[FRAUD].[TBL_MN_PFRAU_MPIO_RECLAMANTE]",conn)
            df_HDO = pd.read_sql_query("SELECT * FROM [SIT_MINERIA].[FRAUD].[TBL_MN_PFRAU_DPTO_OCURRENCIA]",conn)
            df_HDR = pd.read_sql_query("SELECT * FROM [SIT_MINERIA].[FRAUD].[TBL_MN_PFRAU_DPTO_RECLAMANTE]",conn)
            df_HNR = pd.read_sql_query("SELECT * FROM [SIT_MINERIA].[FRAUD].[TBL_MN_PFRAU_NIT_RECLAMANTE]",conn)
        
            bda = bda.merge(df_HMO[['CIUDAD_OCURRENCIA','Cod_ciudad_Ocurrencia']].astype(str),how = 'left',left_on = 'Cod_ciu_Ocurrencia', right_on = 'Cod_ciudad_Ocurrencia')
            bda = bda.merge(df_HMR[['CIUDAD_RECLAMANTE','Cod_ciudad_Reclamante']].astype(str),how = 'left',on = 'Cod_ciudad_Reclamante')
            bda = bda.merge(df_HDO[['DPTO_OCURRENCIA', 'Cod_Depto_Ocurrencia']].astype(str),how = 'left',on = 'Cod_Depto_Ocurrencia')
            bda = bda.merge(df_HDR[['DPTO_RECLAMANTE','Cod_Depto_Reclamante']].astype(str),how = 'left',on = 'Cod_Depto_Reclamante')
            
            bda['NIT Reclamante'] = bda['NIT Reclamante'].astype(str)
            bda = bda.merge(df_HNR[['NIT_RECLAMANTE','NITReclamante']],how = 'left',left_on = 'NIT Reclamante', right_on = 'NITReclamante')
            
            bda['Reclamacion']=bda['Reclamacion'].astype(np.int64)
            bda['NIT Reclamante']=bda['NIT Reclamante'].astype(np.float64)
            bda['Cod_Depto_Ocurrencia']=bda['Cod_Depto_Ocurrencia'].astype(np.float64)
            bda['Cod_ciu_Ocurrencia']=bda['Cod_ciu_Ocurrencia'].astype(np.float64)
            bda['Cod_Depto_Reclamante']=bda['Cod_Depto_Reclamante'].astype(np.float64)
            bda['Cod_ciudad_Reclamante']=bda['Cod_ciudad_Reclamante'].astype(np.float64)
            bda['COND_ACCIDENTADO']=bda['COND_ACCIDENTADO'].str.capitalize()
                
            bda = bda[['TIPO_DOC_RECLAMANTE', 
            'GENERO',
            'ReclamoVSCircula_Dpto', 
            'ReclamoVSCircula_Mpio', 
            'TIPO_VEHICULO',
            'COND_ACCIDENTADO', 
            'CIUDAD_RECLAMANTE', 
            'CIUDAD_OCURRENCIA',
            'DPTO_OCURRENCIA',
            'DPTO_RECLAMANTE',
            'NIT_RECLAMANTE']]

            nombres_esperados = ['TIPO_DOC_RECLAMANTE__CC',
               'TIPO_DOC_RECLAMANTE__NI', 'GENERO__F', 'GENERO__M',
               'ReclamoVSCircula_Dpto__False', 'ReclamoVSCircula_Dpto__True',
               'ReclamoVSCircula_Mpio__False', 'ReclamoVSCircula_Mpio__True',
               'TIPO_VEHICULO__AUTOS', 'TIPO_VEHICULO__CAMPEROS',
               'TIPO_VEHICULO__INTERMUNICIPAL', 'TIPO_VEHICULO__MOTOS',
               'TIPO_VEHICULO__OTROS', 'COND_ACCIDENTADO__Conductor',
               'COND_ACCIDENTADO__Ocupante', 'COND_ACCIDENTADO__Peaton',
               'CIUDAD_RECLAMANTE__MR1', 'CIUDAD_RECLAMANTE__MR2',
               'CIUDAD_RECLAMANTE__MR3', 'CIUDAD_OCURRENCIA__MO1',
               'CIUDAD_OCURRENCIA__MO2', 'CIUDAD_OCURRENCIA__MO3',
               'DPTO_OCURRENCIA__DO1', 'DPTO_OCURRENCIA__DO2', 'DPTO_OCURRENCIA__DO3',
               'DPTO_RECLAMANTE__DR1', 'DPTO_RECLAMANTE__DR2', 'DPTO_RECLAMANTE__DR3',
               'NIT_RECLAMANTE__NR1', 'NIT_RECLAMANTE__NR2']
            
            data_full = pd.get_dummies(bda, columns = ['TIPO_DOC_RECLAMANTE', 'GENERO', 'ReclamoVSCircula_Dpto',
               'ReclamoVSCircula_Mpio', 'TIPO_VEHICULO', 'COND_ACCIDENTADO',
               'CIUDAD_RECLAMANTE', 'CIUDAD_OCURRENCIA',
               'DPTO_OCURRENCIA','DPTO_RECLAMANTE', 'NIT_RECLAMANTE'],
                             prefix = ['TIPO_DOC_RECLAMANTE_', 'GENERO_', 'ReclamoVSCircula_Dpto_',
               'ReclamoVSCircula_Mpio_', 'TIPO_VEHICULO_', 'COND_ACCIDENTADO_',
               'CIUDAD_RECLAMANTE_',  'CIUDAD_OCURRENCIA_',
               'DPTO_OCURRENCIA_','DPTO_RECLAMANTE_', 'NIT_RECLAMANTE_'],
                             drop_first = False,dtype=int)
            data = {'Prueba': ['EJECUTANDO']}
            data= DataFrame(data)
            data.to_csv('E:\InterCambioOperacion\AUTOS\SOAT_FRAUDEV2\model5.csv', index=None)            
    
            ver = [i  not in data_full.columns for i in nombres_esperados]
            ver2 = [x for x,y  in zip(nombres_esperados,ver) if y == True]
    
            for i in ver2:
                data_full[i] = 0

            data_full = data_full[nombres_esperados]
            ### LECTURA DEL OBJETO DEL MODELO #### 
            file_name = "E:\InterCambioOperacion\AUTOS\SOAT_FRAUDEV2\1. Python\Implementacion_Fraude\xgb_fraude_v6"
            xgb_model_loaded = pickle.load(open(file_name, "rb"))
            data = {'Prueba': ['EJECUTANDO']}
            data= DataFrame(data)
            data.to_csv('E:\InterCambioOperacion\AUTOS\SOAT_FRAUDEV2\model6.csv', index=None)            

            probs = xgb_model_loaded.predict_proba(data_full)
            preds = probs[:,1]

            df['SCORE_PREDICCION_V6'] = preds
            df = df.sort_values(by = "SCORE_PREDICCION_V6",ascending = False)

            df = df.reset_index(drop = True)
            df = df.reset_index()
            df['RANKING_PRIORIZACION'] = df['index']+1
            df = df.drop('index',axis = 1)
            
            BASE_MODELO_5 = pd.read_sql_query("""SELECT A.NUMERO_SINIESTRO,CAST(A.NUMERO_RECLAMACION AS VARCHAR) as NUMERO_RECLAMACION, CAST(A.NUMERO_POLIZA AS VARCHAR) AS NUMERO_POLIZA, A.NUMERO_FACTURA, A.FECHA_OCURRENCIA, CAST(A.CEDULA_ACCIDENTADO AS VARCHAR) AS CEDULA_ACCIDENTADO,
            A.HORA_OCURRENCIA, A.EXISTE_RAF,A.IPS_ESQUEMA, A.IND_FRAUDE,  A.FUENTE_FRAUDE, A.ALERTAS, A.FECHA_ESTADO_ACTUAL,A.FECHA_PROCESO, CAST(A.NIT_RECLAMANTE AS VARCHAR) as NIT_RECLAMANTE,
            A.COD_MUNICIPIO_RECLAMANTE AS COD_MUNICIPIO_RECLAMANTE, A.CASO_SOSPECHOSO, A.NRO_DOCUMENTO_ASEGURADO,
            A.FEC_INICIO_VIG_POLIZA,A.ANNO_MODELO,A.EDAD, A.FECHA_AVISO,A.DESC_TIPO_INTERMEDIARIO,A.CONDICION_ACCIDENTADO,
            A.TXT_CAUSA, A.TIPO_RECLAMANTE
            FROM FRAUD.TBL_MN_PFRAU_RECLAMACION_MODEL_V A""",conn)
        
            BASE_SCORE = pd.read_sql_query("SELECT * FROM [SIT_MINERIA].[FRAUD].[TBL_MN_PFRAU_SCORE_IPS]", conn)
            DANE = pd.read_sql_query("SELECT COD_MUNICIPIO AS COD_MUNICIPIO_RECLAMANTE,NOMBRE_DEPARTAMENTO,CABECERA,RESTO  FROM FRAUD.TBL_MN_PFRAU_DANE WHERE ANIO_PROYECCION=(SELECT MAX(ANIO_PROYECCION) FROM FRAUD.TBL_MN_PFRAU_DANE)",conn)
            DANE['URBANA'] = DANE['CABECERA']/(DANE['CABECERA']+DANE['RESTO'])  
            DANE['RURAL'] = DANE['RESTO']/(DANE['CABECERA']+ DANE['RESTO'])
        
            BASE_MODELO_5 = BASE_MODELO_5.merge(DANE[['COD_MUNICIPIO_RECLAMANTE', 'URBANA', 'RURAL','NOMBRE_DEPARTAMENTO']], on="COD_MUNICIPIO_RECLAMANTE", how='left')
            BASE_MODELO_5['ACC_ASEG'] = np.where(BASE_MODELO_5['CEDULA_ACCIDENTADO'] == BASE_MODELO_5['NRO_DOCUMENTO_ASEGURADO'],"EL ASEGURADO ES EL MISMO ACCIDENTADO","EL ACCIDENTADO NO ESTA REGISTRADO COMO TOMADOR DE LA POLIZA")
        
            BASE_MODELO_5['IPS_MPIO'] = BASE_MODELO_5['NIT_RECLAMANTE'].astype(np.int64).astype(str)+'-'+BASE_MODELO_5['COD_MUNICIPIO_RECLAMANTE'].astype(str)
            BASE_MODELO_5['DIST_INICIO_OCURR'] = (pd.to_datetime(BASE_MODELO_5['FECHA_OCURRENCIA'], format='%Y-%m-%d')-pd.to_datetime(BASE_MODELO_5['FEC_INICIO_VIG_POLIZA'], format='%Y-%m-%d')).dt.days
        
            BASE_MODELO_5['GRUPOS_MODELO_VEHICULO'] = np.where(BASE_MODELO_5['ANNO_MODELO'] == 0, "MODELO DEL AUTOMOVIL NO IDENTIFICADO",
            np.where((BASE_MODELO_5['ANNO_MODELO']>2005) & (BASE_MODELO_5['ANNO_MODELO']<=2010),"MODELO 2005-2010",
            np.where(BASE_MODELO_5['ANNO_MODELO']<=2005, "MODELO CON ANTIGUEDAD MENOR A 2005",
            np.where(BASE_MODELO_5['ANNO_MODELO']>2018,"MODELO MAYOR A 2018", BASE_MODELO_5['ANNO_MODELO']))))
        
            BASE_MODELO_5['GRUPOS_EDAD'] = np.where(BASE_MODELO_5['EDAD']==0, "EDAD NO REGISTRADA",
            np.where((BASE_MODELO_5['EDAD']>1) & (BASE_MODELO_5['EDAD']<=15),'GR_EDAD_1-15',
            np.where((BASE_MODELO_5['EDAD']>15) & (BASE_MODELO_5['EDAD']<=20),'GR_EDAD_15-20',
            np.where((BASE_MODELO_5['EDAD']>20) & (BASE_MODELO_5['EDAD']<=25),'GR_EDAD_20-25',
            np.where((BASE_MODELO_5['EDAD']>25) & (BASE_MODELO_5['EDAD']<=30),'GR_EDAD_25-30',
            np.where((BASE_MODELO_5['EDAD']>30) & (BASE_MODELO_5['EDAD']<=35),'GR_EDAD_30-35',
            np.where((BASE_MODELO_5['EDAD']>35) & (BASE_MODELO_5['EDAD']<=40),'GR_EDAD_35-40',
            np.where((BASE_MODELO_5['EDAD']>40) & (BASE_MODELO_5['EDAD']<=45),'GR_EDAD_40-45',
            np.where((BASE_MODELO_5['EDAD']>45) & (BASE_MODELO_5['EDAD']<=50),'GR_EDAD_45-50',
            np.where((BASE_MODELO_5['EDAD']>50) & (BASE_MODELO_5['EDAD']<=55),'GR_EDAD_50-55',
            np.where((BASE_MODELO_5['EDAD']>55) & (BASE_MODELO_5['EDAD']<=60),'GR_EDAD_55-60',
                    'GR_EDAD_60+')))))))))))
            
            BASE_MODELO_5['DIST_OCURR_AVISO'] = (pd.to_datetime(BASE_MODELO_5['FECHA_AVISO'], format='%Y-%m-%d')-pd.to_datetime(BASE_MODELO_5['FECHA_OCURRENCIA'], format = '%Y-%m-%d')).dt.days
    
#            BASE_MODELO_5['GRUPOS_HORA'] = np.where(BASE_MODELO_5['HORA_OCURRENCIA'].astype(str).str[0:2].astype(int) == 0, 'GR_HORA0',
#            np.where((BASE_MODELO_5['HORA_OCURRENCIA'].astype(str).str[0:1].astype(int)>0  ) & (BASE_MODELO_5['HORA_OCURRENCIA'].astype(str).str[0:1].astype(int)<5 ), 'GR_HORA1',
#            np.where((BASE_MODELO_5['HORA_OCURRENCIA'].astype(str).str[0:1].astype(int)>=5 ) & (BASE_MODELO_5['HORA_OCURRENCIA'].astype(str).str[0:1].astype(int)<9 ), 'GR_HORA2',
#            np.where((BASE_MODELO_5['HORA_OCURRENCIA'].astype(str).str[0:1].astype(int)>=9 ) & (BASE_MODELO_5['HORA_OCURRENCIA'].astype(str).str[0:1].astype(int)<11), 'GR_HORA3',
#            np.where((BASE_MODELO_5['HORA_OCURRENCIA'].astype(str).str[0:2].astype(int)>=11) & (BASE_MODELO_5['HORA_OCURRENCIA'].astype(str).str[0:2].astype(int)<14), 'GR_HORA4',
#            np.where((BASE_MODELO_5['HORA_OCURRENCIA'].astype(str).str[0:2].astype(int)>=14) & (BASE_MODELO_5['HORA_OCURRENCIA'].astype(str).str[0:2].astype(int)<17), 'GR_HORA5',
#            np.where((BASE_MODELO_5['HORA_OCURRENCIA'].astype(str).str[0:2].astype(int)>=17) & (BASE_MODELO_5['HORA_OCURRENCIA'].astype(str).str[0:2].astype(int)<20), 'GR_HORA6',
#                    'GR_HORA7')))))))
            
            BASE_MODELO_5 = BASE_MODELO_5.merge(BASE_SCORE, on='IPS_MPIO', how='left')
    
            #a1 Reclamación del Atlantico en que asegurado diferente de accidentado
            BASE_MODELO_5['INTERPRETABILIDAD_SCORE'] = np.where((BASE_MODELO_5['NOMBRE_DEPARTAMENTO']!='ANTIOQUIA') &
            (BASE_MODELO_5['ACC_ASEG'] == 'EL ACCIDENTADO NO ESTA REGISTRADO COMO TOMADOR DE LA POLIZA') & 
            (BASE_MODELO_5['NOMBRE_DEPARTAMENTO'] == 'ATLANTICO'), '001', '')
        
            #a2 Score IPS = 7, Distancia Inicio_Ocurrencia > 267.5 , Modelo_vehiculo NO 2012
            BASE_MODELO_5['INTERPRETABILIDAD_SCORE'] = np.where((BASE_MODELO_5['GRUPO_SCORE_IPS'] == 'r7') & 
            (BASE_MODELO_5['DIST_INICIO_OCURR']>267.5) & 
            (BASE_MODELO_5['GRUPOS_MODELO_VEHICULO']!=2012),
            np.where(BASE_MODELO_5['INTERPRETABILIDAD_SCORE']=='','002',BASE_MODELO_5['INTERPRETABILIDAD_SCORE']+"-"+"002"), 
            BASE_MODELO_5['INTERPRETABILIDAD_SCORE'])
            
            #a3 Departamento Atlantico, Antigüedad vehículo > 2005
            BASE_MODELO_5['INTERPRETABILIDAD_SCORE'] = np.where((BASE_MODELO_5['NOMBRE_DEPARTAMENTO']=='ATLANTICO') & 
            (BASE_MODELO_5['DIST_INICIO_OCURR']<342.5) & 
            (BASE_MODELO_5['GRUPOS_MODELO_VEHICULO']!='MODELO CON ANTIGUEDAD MENOR A 2005'),
            np.where(BASE_MODELO_5['INTERPRETABILIDAD_SCORE']=='',"003",BASE_MODELO_5['INTERPRETABILIDAD_SCORE']+"-"+"003"),
            BASE_MODELO_5['INTERPRETABILIDAD_SCORE'])
        
            #a4 Departamento Atlantico, Grupos Hora 5, Distancia inicio ocurrencia > 342.5 
#            BASE_MODELO_5['INTERPRETABILIDAD_SCORE'] = np.where((BASE_MODELO_5['DIST_INICIO_OCURR']>342.5) & 
#            (BASE_MODELO_5['GRUPOS_HORA']=='GR_HORA0'),
#            np.where(BASE_MODELO_5['INTERPRETABILIDAD_SCORE']=='', "004", BASE_MODELO_5['INTERPRETABILIDAD_SCORE']+"-"+"004"),
#            BASE_MODELO_5['INTERPRETABILIDAD_SCORE'])
        
            #a5 Score IPS 7, Distancia Inicio Ocurrencia > 251
            BASE_MODELO_5['INTERPRETABILIDAD_SCORE'] =np.where((BASE_MODELO_5['GRUPO_SCORE_IPS']=='r7') & 
            (BASE_MODELO_5['NOMBRE_DEPARTAMENTO'] != 'VALLE DEL CAUCA') & 
            (BASE_MODELO_5['DIST_INICIO_OCURR']>251.5),
            np.where(BASE_MODELO_5['INTERPRETABILIDAD_SCORE']=='','005', BASE_MODELO_5['INTERPRETABILIDAD_SCORE']+"-"+"005"), 
            BASE_MODELO_5['INTERPRETABILIDAD_SCORE'])
            
                ##a6 Score IPS 7, Departamento Valle del Cauca, Proporción de población Urbana < 0.92
            BASE_MODELO_5['INTERPRETABILIDAD_SCORE']= np.where((BASE_MODELO_5['GRUPO_SCORE_IPS']=='r7') & 
            (BASE_MODELO_5['NOMBRE_DEPARTAMENTO']=='VALLE DEL CAUCA') & (BASE_MODELO_5['URBANA']<0.9249), 
            np.where(BASE_MODELO_5['INTERPRETABILIDAD_SCORE']=='','006', BASE_MODELO_5['INTERPRETABILIDAD_SCORE']+'-'+"006"),
            BASE_MODELO_5['INTERPRETABILIDAD_SCORE'])
        
            ##a7 Score IPS NO 1, Tipo de Intermediario NO AG Independiente, Departamento Atlantico.
            BASE_MODELO_5['INTERPRETABILIDAD_SCORE']= np.where((BASE_MODELO_5['DESC_TIPO_INTERMEDIARIO']!='AG.INDPTE') & 
            (BASE_MODELO_5['NOMBRE_DEPARTAMENTO']=='ATLANTICO'),
            np.where(BASE_MODELO_5['INTERPRETABILIDAD_SCORE']=='','007', BASE_MODELO_5['INTERPRETABILIDAD_SCORE']+'-'+'007'), 
            BASE_MODELO_5['INTERPRETABILIDAD_SCORE'])
        
            ##A8 Departamento Antioquia, Condición Accidentado NO conductor, Modelo Vehículo <= 2005
            BASE_MODELO_5['INTERPRETABILIDAD_SCORE'] = np.where((BASE_MODELO_5['NOMBRE_DEPARTAMENTO']=='ANTIOQUIA') & 
            (BASE_MODELO_5['CONDICION_ACCIDENTADO']!='CONDUCTOR') & 
            (BASE_MODELO_5['GRUPOS_MODELO_VEHICULO']=='MODELO CON ANTIGUEDAD MENOR A 2005'),
            np.where(BASE_MODELO_5['INTERPRETABILIDAD_SCORE']=='','008', BASE_MODELO_5['INTERPRETABILIDAD_SCORE']+'-'+'008'), 
            BASE_MODELO_5['INTERPRETABILIDAD_SCORE'])
                
                ##A9 # Score IPS NO 1, Condición accidentado peatón.
            BASE_MODELO_5['INTERPRETABILIDAD_SCORE']= np.where((BASE_MODELO_5['GRUPO_SCORE_IPS']!='r1') & 
            (BASE_MODELO_5['CONDICION_ACCIDENTADO']=='PEATON'),
            np.where(BASE_MODELO_5['INTERPRETABILIDAD_SCORE']=='','009', BASE_MODELO_5['INTERPRETABILIDAD_SCORE']+'-'+'009'), 
            BASE_MODELO_5['INTERPRETABILIDAD_SCORE'])
        
            ##A10 Score IPS 2, Edad 40-45, IPS con riesgo Medio-Bajo.
            BASE_MODELO_5['INTERPRETABILIDAD_SCORE']= np.where( (BASE_MODELO_5['GRUPO_SCORE_IPS']=='r2') & 
            (BASE_MODELO_5['GRUPOS_EDAD']=='GR_EDAD_40-45'),
            np.where(BASE_MODELO_5['INTERPRETABILIDAD_SCORE']=='','010', BASE_MODELO_5['INTERPRETABILIDAD_SCORE']+'-'+'010'),
            BASE_MODELO_5['INTERPRETABILIDAD_SCORE'])
        
            ##A11 Edad 35-40, condición accidentado NO conductor, txt causa accidente de transito. 
            BASE_MODELO_5['INTERPRETABILIDAD_SCORE']= np.where( (BASE_MODELO_5['INTERPRETABILIDAD_SCORE']=='GR_EDAD_35-40') & 
            (BASE_MODELO_5['CONDICION_ACCIDENTADO'] !='CONDUCTOR') & (BASE_MODELO_5['TXT_CAUSA']=='ACCIDENTE DE TRANSITO'),
            np.where(BASE_MODELO_5['INTERPRETABILIDAD_SCORE']=='','011', BASE_MODELO_5['INTERPRETABILIDAD_SCORE']+'-'+'011'), 
            BASE_MODELO_5['INTERPRETABILIDAD_SCORE'])
        
            ##A12 # Grupos hora Gr2, Edad NO 30-35
#            BASE_MODELO_5['INTERPRETABILIDAD_SCORE']= np.where((BASE_MODELO_5['GRUPOS_HORA']=='GR_HORA2') & 
#            (BASE_MODELO_5['DIST_OCURR_AVISO']<7.5) & (BASE_MODELO_5['GRUPOS_EDAD']!='GR_EDAD_30-35'),  
#            np.where(BASE_MODELO_5['INTERPRETABILIDAD_SCORE']=='','012', BASE_MODELO_5['INTERPRETABILIDAD_SCORE']+'-'+'012'), 
#            BASE_MODELO_5['INTERPRETABILIDAD_SCORE'])  
                
                ##A13 Departamento la Guajira, Distancia Inicio Ocurrencia > 105.
            BASE_MODELO_5['INTERPRETABILIDAD_SCORE']= np.where((BASE_MODELO_5['NOMBRE_DEPARTAMENTO']=='LA GUAJIRA') & 
            (BASE_MODELO_5['DIST_INICIO_OCURR']>105) & (BASE_MODELO_5['GRUPOS_EDAD']!='GR_EDAD_15-20'),  
            np.where(BASE_MODELO_5['INTERPRETABILIDAD_SCORE']=='','013', BASE_MODELO_5['INTERPRETABILIDAD_SCORE']+'-'+'013'), 
            BASE_MODELO_5['INTERPRETABILIDAD_SCORE'])
        
            ##A14 Distancia Inicio ocurrencia > 319, Departamento NO antioquia, Causa accident= Volcamiento 
            BASE_MODELO_5['INTERPRETABILIDAD_SCORE']= np.where((BASE_MODELO_5['DIST_INICIO_OCURR']>318.5) & 
            (BASE_MODELO_5['NOMBRE_DEPARTAMENTO'] != 'ANTIOQUIA') & (BASE_MODELO_5['TXT_CAUSA']=='VOLCAMIENTO') ,  
            np.where(BASE_MODELO_5['INTERPRETABILIDAD_SCORE']=='','014', BASE_MODELO_5['INTERPRETABILIDAD_SCORE']+'-'+'014'), 
            BASE_MODELO_5['INTERPRETABILIDAD_SCORE'])
        
            ##A15  Distancia Inicio_Ocurrencia > 318.5, Departamento Reclamante Antioquia, Grupos Hora = 1
#            BASE_MODELO_5['INTERPRETABILIDAD_SCORE']= np.where((BASE_MODELO_5['DIST_INICIO_OCURR']>318.5) & 
#            (BASE_MODELO_5['NOMBRE_DEPARTAMENTO']=='ANTIOQUIA') & (BASE_MODELO_5['GRUPOS_HORA']=='GR_HORA0'),  
#            np.where(BASE_MODELO_5['INTERPRETABILIDAD_SCORE']=='','015', BASE_MODELO_5['INTERPRETABILIDAD_SCORE']+'-'+'015'),
#            BASE_MODELO_5['INTERPRETABILIDAD_SCORE'])
        
            ##A16 Grupo Score IPS NO 1, Departamento Magdalena,  Distancia Ocurrencia Aviso > 8.5
            BASE_MODELO_5['INTERPRETABILIDAD_SCORE']= np.where( (BASE_MODELO_5['GRUPO_SCORE_IPS']!='r1') & 
            (BASE_MODELO_5['NOMBRE_DEPARTAMENTO']=='MAGDALENA') & (BASE_MODELO_5['DIST_OCURR_AVISO']>8.5),  
            np.where(BASE_MODELO_5['INTERPRETABILIDAD_SCORE']=='','016', BASE_MODELO_5['INTERPRETABILIDAD_SCORE']+'-'+'016'), 
            BASE_MODELO_5['INTERPRETABILIDAD_SCORE'])
        
            ##A17 Score IPS = 7, Distancia Ocurrencia_Ocurrencia > 267.5 , Modelo_vehiculo NO 2012
            BASE_MODELO_5['INTERPRETABILIDAD_SCORE']= np.where((BASE_MODELO_5['GRUPO_SCORE_IPS']=='r6') & 
            (BASE_MODELO_5['DIST_INICIO_OCURR']>267.5) & (BASE_MODELO_5['GRUPOS_MODELO_VEHICULO'] != 2012),  
            np.where(BASE_MODELO_5['INTERPRETABILIDAD_SCORE']=='','017', BASE_MODELO_5['INTERPRETABILIDAD_SCORE']+'-'+'017'), 
            BASE_MODELO_5['INTERPRETABILIDAD_SCORE'])
            
                ##A18 Score IPS 7, Departamento  Cauca, Proporción de población Urbana < 0.92
            BASE_MODELO_5['INTERPRETABILIDAD_SCORE']= np.where((BASE_MODELO_5['GRUPO_SCORE_IPS']=='r7') & 
            (BASE_MODELO_5['NOMBRE_DEPARTAMENTO']=='CAUCA') & (BASE_MODELO_5['URBANA']<0.9249) ,  
            np.where(BASE_MODELO_5['INTERPRETABILIDAD_SCORE']=='','018', BASE_MODELO_5['INTERPRETABILIDAD_SCORE']+'-'+'018'), 
            BASE_MODELO_5['INTERPRETABILIDAD_SCORE'])
        
            ##A19  # Accidentado NO es el tomador de la poliza, Score IPS 6 o 7, Tipo Reclamante = Clínica 
            BASE_MODELO_5['INTERPRETABILIDAD_SCORE'] = np.where((BASE_MODELO_5['ACC_ASEG']=='EL ACCIDENTADO NO ESTA REGISTRADO COMO TOMADOR DE LA POLIZA') & 
            ((BASE_MODELO_5['GRUPO_SCORE_IPS']=='r6') | (BASE_MODELO_5['GRUPO_SCORE_IPS']=='r7')) & 
            (BASE_MODELO_5['TIPO_RECLAMANTE']=='CLINICA'),  
            np.where(BASE_MODELO_5['INTERPRETABILIDAD_SCORE']=='','019', BASE_MODELO_5['INTERPRETABILIDAD_SCORE']+'-'+'019'), 
            BASE_MODELO_5['INTERPRETABILIDAD_SCORE'])

            data = {'Prueba': ['EJECUTANDO']}
            data= DataFrame(data)
            data.to_csv('E:\InterCambioOperacion\AUTOS\SOAT_FRAUDEV2\model7.csv', index=None)            

        
            cursor.execute("TRUNCATE TABLE [SIT_MINERIA].[STAGE].[STG_MN_PFRAU_PREDICCIONES_VI]")  ####CAMBIAR DIRECCIONAMIENTO
            cursor.commit()
            
            df_predicciones = df[['NUMERO_SINIESTRO','Reclamacion','NUMERO_POLIZA', 'NUMERO_FACTURA','SCORE_PREDICCION_V6','EXISTE_RAF', 
                        'IPS_ESQUEMA', 'IND_FRAUDE', 'FUENTE_FRAUDE', 'ALERTAS', 
            'FECHA_ESTADO_ACTUAL', 'FECHA_PROCESO', 'CASO_SOSPECHOSO']] 
        
            df_predicciones = df_predicciones.merge(BASE_MODELO_5[['NUMERO_SINIESTRO','NUMERO_RECLAMACION','NUMERO_POLIZA','NUMERO_FACTURA','INTERPRETABILIDAD_SCORE']], how='left', 
                                                left_on = ['Reclamacion','NUMERO_POLIZA','NUMERO_FACTURA','NUMERO_SINIESTRO'], 
                                                right_on = ['NUMERO_RECLAMACION','NUMERO_POLIZA','NUMERO_FACTURA','NUMERO_SINIESTRO'])

            for index, row in df_predicciones.iterrows():
                cursor.execute("INSERT INTO [SIT_MINERIA].[STAGE].[STG_MN_PFRAU_PREDICCIONES_VI] ([NUMERO_SINIESTRO], [NUMERO_RECLAMACION], [NUMERO_POLIZA], [NUMERO_FACTURA],[SCORE_PREDICCION], [EXISTE_RAF], [IPS_ESQUEMA], [IND_FRAUDE], [FUENTE_FRAUDE], [ALERTAS], [INTERPRETABILIDAD_SCORE], [FECHA_ESTADO_ACTUAL],[FECHA_PROCESO], [CASO_SOSPECHOSO])" + \
                        " values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?,?,?)",row['NUMERO_SINIESTRO'], row['NUMERO_RECLAMACION'],row['NUMERO_POLIZA'], row['NUMERO_FACTURA'], row['SCORE_PREDICCION_V6'], row['EXISTE_RAF'], row['IPS_ESQUEMA'], row['IND_FRAUDE'], row['FUENTE_FRAUDE'], row['ALERTAS'], row['INTERPRETABILIDAD_SCORE'], row['FECHA_ESTADO_ACTUAL'], row['FECHA_PROCESO'], row['CASO_SOSPECHOSO'])
                cursor.commit()
            data = {'Prueba': ['EJECUTANDO']}
            data= DataFrame(data)
            data.to_csv('E:\InterCambioOperacion\AUTOS\SOAT_FRAUDEV2\model8.csv', index=None)            
                
            ##CARGAR DATOS EN HISTORICO
            cursor.execute("""DELETE FROM FRAUD.TBL_MN_PFRAU_PREDICCIONES_HIS WHERE FECHA_PROCESO =  (SELECT MAX(FECHA_PROCESO) FECHA_PROCESO FROM STAGE.STG_MN_PFRAU_PREDICCIONES_VI)
                        INSERT INTO FRAUD.TBL_MN_PFRAU_PREDICCIONES_HIS 
                        SELECT   CAST(NUMERO_SINIESTRO AS decimal(20)) AS NUMERO_SINIESTRO
                                ,CAST(NUMERO_RECLAMACION AS decimal(20)) AS NUMERO_RECLAMACION
                                ,CAST(NUMERO_POLIZA AS decimal(20)) AS NUMERO_POLIZA
                                ,CAST(NUMERO_FACTURA AS varchar(50)) AS NUMERO_FACTURA
                                ,CAST(SCORE_PREDICCION AS decimal(10,8)) AS SCORE_PREDICCION
                                ,CAST(EXISTE_RAF AS varchar(50)) AS EXISTE_RAF
                                ,CAST(IPS_ESQUEMA AS varchar(50)) AS IPS_ESQUEMA
                                ,CAST(IND_FRAUDE AS varchar(50)) AS IND_FRAUDE
                                ,CAST(ISNULL(FUENTE_FRAUDE,'') AS varchar(50)) AS FUENTE_FRAUDE
                                ,CAST(ALERTAS AS varchar(250)) AS ALERTAS
                                ,'MODELO 5' AS MODELO
                                ,CAST(INTERPRETABILIDAD_SCORE AS varchar(250)) AS INTERPRETABILIDAD_SCORE
                                ,CAST(FECHA_ESTADO_ACTUAL AS varchar(20)) AS FECHA_ESTADO_ACTUAL
                                ,CAST(FECHA_PROCESO AS varchar(10)) AS FECHA_PROCESO
                                ,GETDATE() AS FEC_CREACION
                                ,NULL FEC_MODIFICACION
                            FROM STAGE.STG_MN_PFRAU_PREDICCIONES_VI""")
            cursor.commit()
            
            #PARA GENERAR ARCHIVO DE PREDICCION PARA PREVENCION FRAUDE
            filePredic = pd.read_sql_query("""
            SELECT  
                                         TD1.NUMERO_SINIESTRO
                                        ,TD1.NUMERO_RECLAMACION
                                        ,TD1.NUMERO_POLIZA
                                        ,TD1.NUMERO_FACTURA
                                        ,TD3.FECHA_OCURRENCIA
                                        ,TD3.FECHA_RECP_ASEGURADORA
                                        ,TD3.CEDULA_ACCIDENTADO
                                        ,TD3.HORA_OCURRENCIA
                                        ,TD1.SCORE_PREDICCION
                                        ,TD1.EXISTE_RAF
                                        ,TD1.IPS_ESQUEMA
                                        ,TD1.IND_FRAUDE
                                        ,ISNULL(TD1.FUENTE_FRAUDE,'') AS FUENTE_FRAUDE
                                        ,ISNULL(dbo.FN_MN_PFRAU_ALERTAS(TD1.ALERTAS,'-', 'LINEA'),'') AS ALERTAS
                                        ,ISNULL(dbo.FN_MN_PFRAU_ALERTAS(TD1.INTERPRETABILIDAD_SCORE,'-', 'INTERPRETABILIDAD'),'') AS INTERPRETABILIDAD_SCORE
                                        ,TD1.FECHA_ESTADO_ACTUAL
                                        ,TD1.FECHA_PROCESO
                                        ,TD3.NIT_RECLAMANTE
                                        ,TD3.VALOR_COBRADO
                                        ,TD3.TIPO_RECLAMACION
                                        ,ISNULL(TD2.FECHA_PROCESO,'') AS FECHA_PROCESO_ANT 
                                        ,ISNULL(TD2.SCORE_PREDICCION,0)AS SCORE_PREDICCION_ANT
                                        ,CASE WHEN TD1.CASO_SOSPECHOSO = '' THEN 'NO CUMPLE REGLAS' ELSE 'CUMPLE REGLAS PARA ASIGNAR A PREVENCION FRAUDE' END AS OBSERVACION 
                                        ,(CASE WHEN TD2.NUMERO_RECLAMACION IS NOT NULL AND 
                                            CAST(TD1.SCORE_PREDICCION AS DECIMAL(10,8))<> CAST(TD2.SCORE_PREDICCION AS DECIMAL(10,8)) THEN 'DUPLICADO CAMBIO SCORE' 
                                            WHEN TD2.NUMERO_RECLAMACION IS NOT NULL AND 
                                            CAST(TD1.SCORE_PREDICCION AS DECIMAL(10,8)) = CAST(TD2.SCORE_PREDICCION AS DECIMAL(10,8)) THEN 'DUPLICADO'
                                                WHEN TD2.NUMERO_RECLAMACION IS NULL THEN 'NUEVO'  END) AS VALIDACION_DUPLICADOS
                                    FROM STAGE.STG_MN_PFRAU_PREDICCIONES_VI TD1
                                    LEFT JOIN (SELECT TD1.NUMERO_POLIZA,TD1.NUMERO_RECLAMACION, TD1.NUMERO_SINIESTRO, TD1.SCORE_PREDICCION, MIN(TD1.FECHA_PROCESO) AS FECHA_PROCESO 
                                            FROM FRAUD.TBL_MN_PFRAU_PREDICCIONES_HIS TD1
                                            INNER JOIN (SELECT NUMERO_POLIZA,NUMERO_RECLAMACION, NUMERO_SINIESTRO, MAX(SCORE_PREDICCION) SCORE_PREDICCION 
                                            FROM FRAUD.TBL_MN_PFRAU_PREDICCIONES_HIS 
                                            WHERE FECHA_PROCESO <>  (SELECT MAX(REPLACE(FECHA_PROCESO,'-','')) FECHA_PROCESO 
                                                                     FROM STAGE.STG_MN_PFRAU_PREDICCIONES_VI) 
                                                                     GROUP BY NUMERO_POLIZA,NUMERO_RECLAMACION, NUMERO_SINIESTRO) TD2
                                        ON TD1.NUMERO_RECLAMACION=TD2.NUMERO_RECLAMACION AND TD1.NUMERO_SINIESTRO=TD2.NUMERO_SINIESTRO AND TD1.NUMERO_POLIZA=TD2.NUMERO_POLIZA AND TD1.SCORE_PREDICCION=TD2.SCORE_PREDICCION
                                    GROUP BY TD1.NUMERO_POLIZA,TD1.NUMERO_RECLAMACION, TD1.NUMERO_SINIESTRO, TD1.SCORE_PREDICCION) TD2
                                        ON TD1.NUMERO_RECLAMACION=TD2.NUMERO_RECLAMACION AND TD1.NUMERO_SINIESTRO=TD2.NUMERO_SINIESTRO AND TD1.NUMERO_POLIZA=TD2.NUMERO_POLIZA
                                    LEFT JOIN (SELECT DISTINCT NUMERO_POLIZA,NUMERO_SINIESTRO,NUMERO_RECLAMACION,NUMERO_FACTURA,FECHA_OCURRENCIA,FECHA_RECP_ASEGURADORA,CEDULA_ACCIDENTADO,HORA_OCURRENCIA, NIT_RECLAMANTE, VALOR_COBRADO,TIPO_RECLAMACION  FROM FRAUD.TBL_MN_PFRAU_RECLAMACION WHERE NUMERO_FACTURA <> '' ) TD3
                                        ON TD1.NUMERO_RECLAMACION=TD3.NUMERO_RECLAMACION AND TD1.NUMERO_SINIESTRO=TD3.NUMERO_SINIESTRO AND TD1.NUMERO_POLIZA=TD3.NUMERO_POLIZA AND TD1.NUMERO_FACTURA = TD3.NUMERO_FACTURA""",conn)

            df['NUMERO_SINIESTRO']=df['NUMERO_SINIESTRO'].astype(float).astype(int).astype(str)
            
            filePredic['NUMERO_SINIESTRO']=filePredic['NUMERO_SINIESTRO'].astype(str)
            filePredic['NUMERO_RECLAMACION']=filePredic['NUMERO_RECLAMACION'].astype(str)
            filePredic['NUMERO_POLIZA']=filePredic['NUMERO_POLIZA'].astype(str)

            filePredic2 = filePredic.merge(df[['NUMERO_SINIESTRO','Reclamacion','NUMERO_POLIZA','NUMERO_FACTURA','RANKING_PRIORIZACION']], how='left', 
                                            left_on = ['NUMERO_RECLAMACION','NUMERO_POLIZA','NUMERO_FACTURA','NUMERO_SINIESTRO'], 
                                            right_on = ['Reclamacion','NUMERO_POLIZA','NUMERO_FACTURA','NUMERO_SINIESTRO'])
            filePredic2=filePredic2.sort_values(by='SCORE_PREDICCION', ascending=False)
            filePredic2 = filePredic2.reset_index(drop = True)
            filePredic2 = filePredic2.reset_index()
            filePredic2['RANKING_PRIORIZACION'] = filePredic2['index']+1
            filePredic2 = filePredic2.drop('index',axis = 1)

            filePredic2=filePredic2.drop(['Reclamacion'], axis=1)
            filePredic2['PROPENSION_FRAUDE']=np.where(filePredic2['SCORE_PREDICCION'].astype(float)>=float(valor_corte.replace(',','.')),1,0)
            data = {'Prueba': ['EJECUTANDO']}
            data= DataFrame(data)
            data.to_csv('E:\InterCambioOperacion\AUTOS\SOAT_FRAUDEV2\model9.csv', index=None)            

            #PARA GENERAR ARCHIVO DE PREDICCION PARA LINEA
            filePredicLine = pd.read_sql_query("""
                                       SELECT  
                                         TD1.NUMERO_SINIESTRO
                                        ,TD1.NUMERO_RECLAMACION
                                        ,TD1.NUMERO_POLIZA
                                        ,TD1.NUMERO_FACTURA
                                        ,TD3.FECHA_OCURRENCIA
                                        ,TD3.FECHA_RECP_ASEGURADORA
                                        ,TD3.CEDULA_ACCIDENTADO
                                        ,TD3.HORA_OCURRENCIA
                                        ,TD1.SCORE_PREDICCION
                                        ,TD1.EXISTE_RAF
                                        ,TD1.IPS_ESQUEMA
                                        ,TD1.IND_FRAUDE
                                        ,ISNULL(TD1.FUENTE_FRAUDE,'') AS FUENTE_FRAUDE
                                        ,ISNULL(dbo.FN_MN_PFRAU_ALERTAS(TD1.ALERTAS,'-', 'LINEA'),'') AS ALERTAS
                                        ,ISNULL(dbo.FN_MN_PFRAU_ALERTAS(TD1.INTERPRETABILIDAD_SCORE,'-', 'INTERPRETABILIDAD'),'') AS INTERPRETABILIDAD_SCORE
                                        ,TD1.FECHA_ESTADO_ACTUAL
                                        ,TD1.FECHA_PROCESO
                                        ,TD3.NIT_RECLAMANTE
                                        ,TD3.VALOR_COBRADO
                                        ,TD3.TIPO_RECLAMACION
                                        ,ISNULL(TD2.FECHA_PROCESO,'') AS FECHA_PROCESO_ANT 
                                        ,ISNULL(TD2.SCORE_PREDICCION,0)AS SCORE_PREDICCION_ANT
                                        ,CASE WHEN TD1.CASO_SOSPECHOSO = '' THEN 'NO CUMPLE REGLAS' ELSE 'CUMPLE REGLAS PARA ASIGNAR A PREVENCION FRAUDE' END AS OBSERVACION 
                                        ,(CASE WHEN TD2.NUMERO_RECLAMACION IS NOT NULL AND 
                                            CAST(TD1.SCORE_PREDICCION AS DECIMAL(10,8))<> CAST(TD2.SCORE_PREDICCION AS DECIMAL(10,8)) THEN 'DUPLICADO CAMBIO SCORE' 
                                            WHEN TD2.NUMERO_RECLAMACION IS NOT NULL AND 
                                            CAST(TD1.SCORE_PREDICCION AS DECIMAL(10,8)) = CAST(TD2.SCORE_PREDICCION AS DECIMAL(10,8)) THEN 'DUPLICADO'
                                                WHEN TD2.NUMERO_RECLAMACION IS NULL THEN 'NUEVO'  END) AS VALIDACION_DUPLICADOS
                                    FROM STAGE.STG_MN_PFRAU_PREDICCIONES_VI TD1
                                    LEFT JOIN (SELECT TD1.NUMERO_POLIZA,TD1.NUMERO_RECLAMACION, TD1.NUMERO_SINIESTRO, TD1.SCORE_PREDICCION, MIN(TD1.FECHA_PROCESO) AS FECHA_PROCESO 
                                            FROM FRAUD.TBL_MN_PFRAU_PREDICCIONES_HIS TD1
                                            INNER JOIN (SELECT NUMERO_POLIZA,NUMERO_RECLAMACION, NUMERO_SINIESTRO, MAX(SCORE_PREDICCION) SCORE_PREDICCION 
                                            FROM FRAUD.TBL_MN_PFRAU_PREDICCIONES_HIS 
                                            WHERE FECHA_PROCESO <>  (SELECT MAX(REPLACE(FECHA_PROCESO,'-','')) FECHA_PROCESO 
                                                                     FROM STAGE.STG_MN_PFRAU_PREDICCIONES_VI) 
                                                                     GROUP BY NUMERO_POLIZA,NUMERO_RECLAMACION, NUMERO_SINIESTRO) TD2
                                        ON TD1.NUMERO_RECLAMACION=TD2.NUMERO_RECLAMACION AND TD1.NUMERO_SINIESTRO=TD2.NUMERO_SINIESTRO AND TD1.NUMERO_POLIZA=TD2.NUMERO_POLIZA AND TD1.SCORE_PREDICCION=TD2.SCORE_PREDICCION
                                    GROUP BY TD1.NUMERO_POLIZA,TD1.NUMERO_RECLAMACION, TD1.NUMERO_SINIESTRO, TD1.SCORE_PREDICCION) TD2
                                        ON TD1.NUMERO_RECLAMACION=TD2.NUMERO_RECLAMACION AND TD1.NUMERO_SINIESTRO=TD2.NUMERO_SINIESTRO AND TD1.NUMERO_POLIZA=TD2.NUMERO_POLIZA
                                    LEFT JOIN (SELECT DISTINCT NUMERO_POLIZA,NUMERO_SINIESTRO,NUMERO_RECLAMACION,NUMERO_FACTURA,FECHA_OCURRENCIA,FECHA_RECP_ASEGURADORA,CEDULA_ACCIDENTADO,HORA_OCURRENCIA, NIT_RECLAMANTE, VALOR_COBRADO,TIPO_RECLAMACION  FROM FRAUD.TBL_MN_PFRAU_RECLAMACION WHERE NUMERO_FACTURA <> ''  ) TD3
                                        ON TD1.NUMERO_RECLAMACION=TD3.NUMERO_RECLAMACION AND TD1.NUMERO_SINIESTRO=TD3.NUMERO_SINIESTRO AND TD1.NUMERO_POLIZA=TD3.NUMERO_POLIZA AND TD1.NUMERO_FACTURA = TD3.NUMERO_FACTURA""",conn)
            
            data = {'Prueba': ['EJECUTANDO']}
            data= DataFrame(data)
            data.to_csv('E:\InterCambioOperacion\AUTOS\SOAT_FRAUDEV2\model10.csv', index=None)            
            
            if len(filePredic)>0:
                fileSeguimiento = pd.read_sql_query("SELECT FECHA_PROCESO, TIPO, CANT_REG_ANALIZADOS, CANT_REG_GENERADOS FROM SIT_MINERIA.FRAUD.TBL_MN_PFRAU_SEGUIM_MODELOS WHERE TIPO ='MODELO V' ORDER BY 1 DESC", conn)

                #os.chdir(output_file_path)
                nombre_salida = "PREDICCIONES_V6_" + TodaysDate + ".csv"
                filePredic2.to_csv('E:\InterCambioOperacion\AUTOS\SOAT_FRAUDEV2\filePredic2.csv', index=None)
                fileSeguimiento.to_csv('E:\InterCambioOperacion\AUTOS\SOAT_FRAUDEV2\fileSeguimiento.csv', index=None)
                filePredic2.to_csv('\\\\dc1pcadfrs1\\IntercambioTerceros\\AUTOS\\SOAT_FRAUDE\\Output\\'+nombre_salida,sep = ";",index = False)
                fileSeguimiento.to_csv('\\\\dc1pcadfrs1\\IntercambioTerceros\\AUTOS\\SOAT_FRAUDE\\Output\\SeguimientoModelos.csv', sep=";", index=False)

#                filePredic2.to_csv('//dc2tvaftp1/IntercambioTerceros/AUTOS/SOAT_FRAUDE/Output/'+nombre_salida,sep = ";",index = False)
#                fileSeguimiento.to_csv('//dc2tvaftp1/IntercambioTerceros/AUTOS/SOAT_FRAUDE/Output/SeguimientoModelos.csv', sep=";", index=False)

        
        cursor.close()
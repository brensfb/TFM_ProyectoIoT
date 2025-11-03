import pandas as pd
from datetime import datetime, timedelta, timezone
import pulp
from influxdb_client import InfluxDBClient
from sqlalchemy import create_engine
import warnings
from influxdb_client.client.warnings import MissingPivotFunction
import pyodbc
import logging
import time
import os
from dotenv import load_dotenv

from mainTrayectosTFM import obtener_eventos_por_ids, obtener_eventos_influx, actualizar_evento_con_trayecto

# CARGAR VARIABLES DE ENTORNO
load_dotenv()

warnings.simplefilter("ignore", MissingPivotFunction)

# Configurar logging
logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

FECHA_DEFECTO = datetime.now(timezone.utc).replace(tzinfo=None)
HORAS_DEFECTO = 12

def obtener_eventos_programados(fecha, horas, barco=None):
    try:
        # USAR VARIABLES DE ENTORNO
        sql_server = os.getenv('SQL_SERVER')
        sql_username = os.getenv('SQL_USERNAME')
        sql_password = os.getenv('SQL_PASSWORD')
        sql_database = os.getenv('SQL_DATABASE')
        sql_driver = os.getenv('SQL_DRIVER')
        
        conn_rms = create_engine(f"mssql+pyodbc://{sql_username}:{sql_password}@{sql_server}/{sql_database}?driver={sql_driver.replace(' ', '+')}&TrustServerCertificate=yes")

        fecha_ini = (fecha - timedelta(hours=horas)).strftime('%Y%m%d %H:%M:%S')
        fecha_fin = min(fecha + timedelta(hours=horas), datetime.now(timezone.utc).replace(tzinfo=None)).strftime('%Y%m%d %H:%M:%S')

        filtro_barco = f"and barco = '{barco}'" if barco else ""
        sql_query = f"""
            declare @fechaIni datetime = '{fecha_ini}'
            declare @fechaFin datetime = '{fecha_fin}'

            ;with programacion as(
            SELECT *, 
                (CONVERT(datetime, Fecha) + CONVERT(datetime, Hora)) AT TIME ZONE 'GMT Standard Time' AT TIME ZONE 'UTC' AS fechaSalidaUTC,
                (CONVERT(datetime, dateadd(day, dias, Fecha)) + CONVERT(datetime, llegada)) AT TIME ZONE 'GMT Standard Time' AT TIME ZONE 'UTC' AS fechaLlegadaUTC
            FROM rms.dbo.fn_ProgramacionRpl(dateadd(hour, -2, @fechaIni), dateadd(hour, 2, @fechaFin) )
            WHERE linea NOT IN (10) and Escalas = 0 {filtro_barco}
            )
            select 'Salida' as tipo_evento, fechaSalidaUTC as fecha_evento, b.puertoLocode as puerto, barco
            from programacion A
            inner join [Mars].[dbo].[PuertosBravo] B on LEFT(trayecto,3) = B.puertoBravo COLLATE Modern_Spanish_CI_AS
            where Escalas = 0 and (fechaSalidaUTC >=  @fechaIni and fechaSalidaUTC <= dateadd(MINUTE, 20, @fechaFin))
            union
            select 'Llegada' as tipo_evento, fechaLlegadaUTC as fecha_evento, b.puertoLocode as puerto, barco
            from programacion A
            inner join [Mars].[dbo].[PuertosBravo] B on right(trayecto,3) = B.puertoBravo COLLATE Modern_Spanish_CI_AS
            where Escalas = 0 and (fechaLlegadaUTC >=  @fechaIni and fechaLlegadaUTC <= dateadd(MINUTE, 20, @fechaFin))
            order by fecha_evento
        """
        df = pd.read_sql(sql_query, conn_rms)
        
        df['fecha_evento'] = pd.to_datetime(df['fecha_evento'])
        df['barco'] = df['barco'].str.strip()
        df['puerto'] = df['puerto'].str.strip()

        if df['fecha_evento'].dt.tz is None:
            df['fecha_evento'] = df['fecha_evento'].dt.tz_localize('UTC')
        else:
            df['fecha_evento'] = df['fecha_evento'].dt.tz_convert('UTC')
        
        return df

    except Exception as e:
        logger.error(f"Error al consultar eventos programados: {e}")
        return pd.DataFrame()

def obtener_eventos_detectados(fecha, horas, barco=None):
    try:
        # USAR VARIABLES DE ENTORNO
        influx_url = os.getenv('INFLUX_URL')
        influx_token = os.getenv('INFLUX_TOKEN')
        influx_org = os.getenv('INFLUX_ORG')
        influx_bucket = os.getenv('INFLUX_BUCKET')

        client = InfluxDBClient(url=influx_url, token=influx_token, org=influx_org)
        query_api = client.query_api()

        fecha_ini = (fecha - timedelta(hours=horas)).replace(tzinfo=timezone.utc)
        fecha_fin = min((fecha + timedelta(hours=horas) + timedelta(minutes=1)).replace(tzinfo=timezone.utc)
                        , datetime.now(timezone.utc) + timedelta(minutes=1))
        fecha_ini_str = fecha_ini.isoformat("T")
        fecha_fin_str = fecha_fin.isoformat("T") 

        filtro_barco_flux = f'|> filter(fn: (r) => strings.substring(v:strings.toUpper(v: r.host), start: strings.strlen(v: r.host) - 3, end: strings.strlen(v: r.host)) == "{barco}")' if barco else ''
        flux_query = f"""
                import "strings"
                baseData =
                    from(bucket: "{influx_bucket}")
                        |> range(start: {fecha_ini_str}, stop: {fecha_fin_str})
                        |> filter(fn: (r) => r["_measurement"] == "events")
                        |> filter(fn: (r) => r["_field"] == "navigationalStatusEvent")
                        {filtro_barco_flux}
                        |> map(fn: (r) => ({{fecha_evento: r._time
                                ,puerto:r.port
                                ,tipo_evento: r._value
                                ,barco: strings.substring(v:strings.toUpper(v: r.host), start: strings.strlen(v: r.host) - 3, end: strings.strlen(v: r.host))
                                }}))
                baseData
        """

        df_or_list = query_api.query_data_frame(query=flux_query)
        client.close()

        if isinstance(df_or_list, list):
            df = pd.concat(df_or_list, ignore_index=True) if df_or_list else pd.DataFrame()
        else:
            df = df_or_list

        if df.empty:
            return pd.DataFrame()

        df['fecha_evento'] = pd.to_datetime(df['fecha_evento'])
        df['barco'] = df['barco'].str.strip()
        df['puerto'] = df['puerto'].str.strip()

        if df['fecha_evento'].dt.tz is None:
            df['fecha_evento'] = df['fecha_evento'].dt.tz_localize('UTC')
        else:
            df['fecha_evento'] = df['fecha_evento'].dt.tz_convert('UTC')

        return df

    except Exception as e:
        logger.error(f"Error al consultar InfluxDB: {e}")
        return pd.DataFrame()

def filtrar_eventos_detectados_pares(df):
    if df.empty:
        return df
    
    # Ordenar por barco y fecha
    df_ordenado = df.sort_values(['barco', 'fecha_evento']).reset_index(drop=True)
    eventos_validos = []
    eliminados = 0
    
    for barco, grupo in df_ordenado.groupby('barco'):
        grupo = grupo.reset_index(drop=True)
        if len(grupo) == 1:
            eventos_validos.append(grupo.iloc[0])
            continue
        
        # Primer evento siempre válido
        validos_barco = [grupo.iloc[0]]
        
        for i in range(1, len(grupo)):
            actual = grupo.iloc[i]
            # Comparar con TODOS los eventos ya validados
            es_valido = True
            for validado in validos_barco:
                diferencia_min = abs((actual['fecha_evento'] - validado['fecha_evento']).total_seconds() / 60)
                
                # BHC: sin restricción | Otros: mínimo 3 minutos
                umbral = 0 if barco == 'BCH' else 3
                
                if diferencia_min < umbral:
                    es_valido = False
                    eliminados += 1
                    break
            
            if es_valido:
                validos_barco.append(actual)
        
        eventos_validos.extend(validos_barco)
    
    df_final = pd.DataFrame(eventos_validos).reset_index(drop=True) if eventos_validos else pd.DataFrame()

    return df_final

def emparejar_eventos(df_programados, df_detectados, umbral_minutos):
    columnas_join = ['barco', 'tipo_evento', 'puerto']
    df_candidatos = pd.merge(
        df_programados.reset_index(),
        df_detectados.reset_index(),
        on=columnas_join,
        suffixes=('_prog', '_det')
    )
    
    df_candidatos['diferencia'] = (df_candidatos['fecha_evento_det'] - df_candidatos['fecha_evento_prog']).abs()
    umbral_td = timedelta(minutes=umbral_minutos)
    df_candidatos = df_candidatos[df_candidatos['diferencia'] <= umbral_td]

    if not df_candidatos.empty:
        df_candidatos_ordenado = df_candidatos.sort_values('diferencia')
        aceptados_por_barco = {}
        indices_a_mantener = []

        for idx, candidato_actual in df_candidatos_ordenado.iterrows():
            barco = candidato_actual['barco']
            if barco not in aceptados_por_barco:
                aceptados_por_barco[barco] = []

            tiene_conflicto = False
            for aceptado in aceptados_por_barco[barco]:
                cruzan_en_tiempo = \
                    (candidato_actual['fecha_evento_prog'] < aceptado['fecha_evento_prog'] and
                     candidato_actual['fecha_evento_det'] > aceptado['fecha_evento_det']) or \
                    (candidato_actual['fecha_evento_prog'] > aceptado['fecha_evento_prog'] and
                     candidato_actual['fecha_evento_det'] < aceptado['fecha_evento_det'])
                if cruzan_en_tiempo:
                    tiene_conflicto = True
                    break
            
            if not tiene_conflicto:
                aceptados_por_barco[barco].append(candidato_actual)
                indices_a_mantener.append(idx)

        df_candidatos = df_candidatos.loc[indices_a_mantener]

    posibles_pares = list(zip(df_candidatos['index_prog'], df_candidatos['index_det']))
    costos = dict(zip(posibles_pares, (df_candidatos['diferencia'].dt.total_seconds() / 60)))

    if not posibles_pares:
        return pd.DataFrame(columns=['barco', 'puerto', 'tipo_evento', 'fecha_programada', 'fecha_detectada', 'diferencia_minutos']), df_programados.copy(), df_detectados.copy()

    prob = pulp.LpProblem("Emparejamiento_de_Eventos", pulp.LpMaximize)
    choices = pulp.LpVariable.dicts("Eleccion", posibles_pares, cat='Binary')

    M = umbral_minutos + 1
    prob += pulp.lpSum(
        [(M - costos[p]) * choices[p] for p in posibles_pares]
    ), "Maximizar_Emparejamientos_Minimizando_Coste"

    prog_indices_unicos = df_programados.index.unique()
    for i in prog_indices_unicos:
        prob += pulp.lpSum([choices[(pi, j)] for pi, j in posibles_pares if pi == i]) <= 1, f"Prog_un_match_{i}"

    det_indices_unicos = df_detectados.index.unique()
    for j in det_indices_unicos:
        prob += pulp.lpSum([choices[(i, dj)] for i, dj in posibles_pares if dj == j]) <= 1, f"Det_un_match_{j}"

    prob.solve(pulp.PULP_CBC_CMD(msg=0))

    eventos_emparejados = []
    indices_prog_emparejados = set()
    indices_det_emparejados = set()

    for par in posibles_pares:
        if choices[par].varValue == 1:
            idx_prog, idx_det = par
            prog_row = df_programados.loc[idx_prog]
            det_row = df_detectados.loc[idx_det]
            
            evento_emparejado = {
                'barco': prog_row['barco'],
                'puerto': prog_row['puerto'],
                'tipo_evento': prog_row['tipo_evento'],
                'fecha_programada': prog_row['fecha_evento'],
                'fecha_detectada': det_row['fecha_evento'],
                'diferencia_minutos': int(costos[par])
            }
            eventos_emparejados.append(evento_emparejado)
            indices_prog_emparejados.add(idx_prog)
            indices_det_emparejados.add(idx_det)

    df_emparejados = pd.DataFrame(eventos_emparejados)
    df_programados_no_detectados = df_programados[~df_programados.index.isin(indices_prog_emparejados)].reset_index(drop=True)
    df_detectados_no_programados = df_detectados[~df_detectados.index.isin(indices_det_emparejados)].reset_index(drop=True)
    
    return df_emparejados, df_programados_no_detectados, df_detectados_no_programados

def main(fecha=None, horas=None, barco=None):
    inicio_total = time.time()
    try:
        inicio_prep = time.time()
        if fecha is None:
            fecha_inicio = FECHA_DEFECTO
        elif isinstance(fecha, str):
            # Aceptar tanto YYYY-MM-DD como ISO datetime
            if 'T' in fecha:
                fecha_inicio = datetime.fromisoformat(fecha.replace('Z', '+00:00')).replace(tzinfo=None)
            else:
                fecha_inicio = datetime.strptime(fecha, '%Y-%m-%d')
        else:
            fecha_inicio = fecha

        umbral_horas = horas if horas is not None else HORAS_DEFECTO
        umbral_minutos = umbral_horas * 60
        print(f"1. Preparación completada: {time.time() - inicio_prep:.2f}s")

        # DEBUG: Mostrar fecha de referencia
        #print(f"REFERENCIA - Fecha evento: {fecha_inicio}")
        #print(f"REFERENCIA - Umbral: ±{umbral_horas} horas")
        #print(f"REFERENCIA - Buscando desde: {fecha_inicio - timedelta(hours=umbral_horas)} hasta: {fecha_inicio + timedelta(hours=umbral_horas)}")
        inicio_prog = time.time()
        df_programados = obtener_eventos_programados(fecha_inicio, umbral_horas, barco=barco)
        print(f"2. Eventos programados obtenidos: {time.time() - inicio_prog:.2f}s")
        inicio_det = time.time()
        df_detectados_sin_filtrar = obtener_eventos_detectados(fecha_inicio, umbral_horas, barco=barco)
        df_detectados = filtrar_eventos_detectados_pares(df_detectados_sin_filtrar)
        print(f"3. Eventos detectados obtenidos y filtrados: {time.time() - inicio_det:.2f}s")
        inicio_emp = time.time()
        df_emparejados, df_programados_no_detectados, df_detectados_no_programados = emparejar_eventos(df_programados, df_detectados, umbral_minutos)
        print(f"4. Eventos emparejados: {time.time() - inicio_emp:.2f}s")

        inicio_proc = time.time()
        # Unificar resultados
        df1 = df_emparejados.copy()
        if not df1.empty:
            df1['difMin'] = ((df1['fecha_detectada'] - df1['fecha_programada']).dt.total_seconds() / 60).round().astype('Int64')
        else:
            df1['difMin'] = ''
        df1['fechaProgramada'] = df1['fecha_programada']
        df1['fechaDetectada'] = df1['fecha_detectada']
        df1['tipoEvento'] = df1['tipo_evento']
        df1 = df1[['barco', 'puerto', 'tipoEvento', 'fechaProgramada', 'fechaDetectada', 'difMin']]

        df2 = df_programados_no_detectados.copy()
        df2['fechaDetectada'] = ''
        df2['difMin'] = ''
        df2['fechaProgramada'] = df2['fecha_evento']
        df2['tipoEvento'] = df2['tipo_evento']
        df2 = df2[['barco', 'puerto', 'tipoEvento', 'fechaProgramada', 'fechaDetectada', 'difMin']]

        df3 = df_detectados_no_programados.copy()
        df3['fechaProgramada'] = ''
        df3['difMin'] = ''
        df3['fechaDetectada'] = df3['fecha_evento']
        df3['tipoEvento'] = df3['tipo_evento']
        df3 = df3[['barco', 'puerto', 'tipoEvento', 'fechaProgramada', 'fechaDetectada', 'difMin']]

        dfs = [df for df in [df1, df2, df3] if not df.empty]
        df_final = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
        print(f"5. Procesamiento de resultados: {time.time() - inicio_proc:.2f}s")

        inicio_sql = time.time()
        resultado_merge = merge_eventos_sql(df_final)
        print(f"6. Merge a SQL completado: {time.time() - inicio_sql:.2f}s")

        tiempo_total = time.time() - inicio_total
        print(f"TIEMPO TOTAL DE EJECUCIÓN: {tiempo_total:.2f} segundos")
        
        return df_final, resultado_merge

    except Exception as e:
        tiempo_total = time.time() - inicio_total
        print(f"TIEMPO TOTAL DE EJECUCIÓN (con error): {tiempo_total:.2f} segundos")
        logger.error(f"Error en función main: {e}")
        return None, {'total_afectadas': 0,'cambio': 0,'ids': []}

def merge_eventos_sql(df_final):
    try:
        # USAR VARIABLES DE ENTORNO PARA PYODBC
        sql_server = os.getenv('SQL_SERVER')
        sql_username = os.getenv('SQL_USERNAME')
        sql_password = os.getenv('SQL_PASSWORD')
        sql_database = os.getenv('SQL_DATABASE')
        sql_driver = os.getenv('SQL_DRIVER')
        
        conn = pyodbc.connect(
            f'DRIVER={{{sql_driver}}};SERVER={sql_server};DATABASE={sql_database};UID={sql_username};PWD={sql_password};TrustServerCertificate=yes'
        )
        cursor = conn.cursor()

        cursor.execute("""
            IF OBJECT_ID('tempdb..#eventos_temp') IS NOT NULL DROP TABLE #eventos_temp;
            CREATE TABLE #eventos_temp (
                barco NVARCHAR(3),
                puerto NVARCHAR(3),
                tipoEvento NVARCHAR(10),
                fechaProgramada DATETIMEOFFSET(7) NULL,
                fechaDetectada DATETIMEOFFSET(7) NULL,
                difMin INT NULL,
                fum DATETIMEOFFSET(7) NULL
            );
            """)
        conn.commit()

        def clean(val):
            if pd.isna(val) or val == '':
                return None
            return val
        
        now_utc = datetime.now(timezone.utc)
        data = [
            (
                clean(row['barco']),
                clean(row['puerto']),
                clean(row['tipoEvento']),
                clean(row['fechaProgramada']),
                clean(row['fechaDetectada']),
                int(row['difMin']) if not pd.isna(row['difMin']) and row['difMin'] != '' else None,
                now_utc
            )
            for _, row in df_final.iterrows()
        ]

        cursor.fast_executemany = True
        cursor.executemany('''
            INSERT INTO #eventos_temp (barco, puerto, tipoEvento, fechaProgramada, fechaDetectada, difMin, fum)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', data)
        conn.commit()

        # CREAR TABLA TEMPORAL PARA CAPTURAR OUTPUT
        cursor.execute("""
            IF OBJECT_ID('tempdb..#merge_output') IS NOT NULL DROP TABLE #merge_output;
            CREATE TABLE #merge_output (
                TipoOperacion NVARCHAR(10),
                EventoID INT,
                barco NVARCHAR(3),
                puerto NVARCHAR(3),
                tipoEvento NVARCHAR(10),
                fechaProgramada DATETIMEOFFSET(7) NULL,
                fechaDetectada DATETIMEOFFSET(7) NULL,
                difMin INT NULL,
                fum DATETIMEOFFSET(7) NULL
            );
        """)
        conn.commit()

    
        merge_sql = '''
        MERGE [Mars].[dbo].[eventos] AS target
        USING #eventos_temp AS source
             ON target.barco = source.barco
            AND target.puerto = source.puerto
            AND target.tipoEvento = source.tipoEvento
            AND (
                (target.fechaProgramada = source.fechaProgramada AND source.fechaProgramada IS NOT NULL) 
                OR 
                (target.fechaDetectada = source.fechaDetectada AND source.fechaDetectada IS NOT NULL) )
        WHEN MATCHED 
        AND ((target.fechaDetectada IS NULL AND source.fechaDetectada IS NOT NULL) 
                OR 
                (target.fechaProgramada IS NULL AND source.fechaProgramada IS NOT NULL)
                OR -- si la fecha programada es la misma, pero la detectada es diferente y la nueva detectada es mejor
                (target.fechaProgramada = source.fechaProgramada 
                    AND source.fechaDetectada IS NOT NULL 
                    AND source.difMin IS NOT NULL
                    AND (target.difMin IS NULL OR ABS(source.difMin) < ABS(target.difMin))
                    AND target.fechaDetectada <> source.fechaDetectada))
        THEN UPDATE SET
               target.fechaDetectada = 
                CASE 
                    WHEN target.fechaDetectada IS NULL AND source.fechaDetectada IS NOT NULL THEN source.fechaDetectada
                    ELSE target.fechaDetectada
                END,
                target.fechaProgramada = 
                    CASE 
                        WHEN target.fechaProgramada IS NULL AND source.fechaProgramada IS NOT NULL THEN source.fechaProgramada
                        ELSE target.fechaProgramada
                    END,
                target.difMin = source.difMin,
                target.fum = source.fum
        WHEN NOT MATCHED BY TARGET
        THEN INSERT (barco, puerto, tipoEvento, fechaProgramada, fechaDetectada, difMin, fum)
             VALUES (source.barco, source.puerto, source.tipoEvento, source.fechaProgramada, source.fechaDetectada, source.difMin, source.fum)
        OUTPUT 
            $action AS TipoOperacion,
            CASE 
                WHEN $action = 'INSERT' THEN inserted.id 
                WHEN $action = 'UPDATE' THEN inserted.id 
            END AS EventoID,
            inserted.barco,
            inserted.puerto,
            inserted.tipoEvento,
            inserted.fechaProgramada,
            inserted.fechaDetectada,
            inserted.difMin,
            inserted.fum
        INTO #merge_output;
        '''
        cursor.execute(merge_sql)
        conn.commit()

        # LEER LOS RESULTADOS DEL OUTPUT
        cursor.execute("SELECT TipoOperacion, EventoID, barco, puerto, tipoEvento, CONVERT(VARCHAR, fechaProgramada, 120) as fechaProgramada, CONVERT(VARCHAR, fechaDetectada, 120) as fechaDetectada, difMin FROM #merge_output;")
        results = cursor.fetchall()

        # RESUMEN ESQUEMATIZADO
        insertados = []
        actualizados = []
        ids = []
        
        if results:
            for result in results:
                action, evento_id, barco, puerto, tipo_evento, fecha_prog, fecha_det, dif_min = result
                registro = f"{barco}-{tipo_evento}-{puerto}-{fecha_prog}-{fecha_det}"
                
                if action == 'INSERT':
                    insertados.append(registro)
                elif action == 'UPDATE':
                    actualizados.append(registro)
                if evento_id is not None:
                    ids.append(evento_id)

        if actualizados:
            print(f"Actualizados {len(actualizados)}: {', '.join(actualizados)}")
        if insertados:
            print(f"Nuevos {len(insertados)}: {', '.join(insertados)}")

        total_afectadas = len(insertados) + len(actualizados)

        # LIMPIAR TABLAS TEMPORALES
        cursor.execute('DROP TABLE #eventos_temp;')
        cursor.execute('DROP TABLE #merge_output;')
        conn.commit()
        cursor.close()
        conn.close()

        return {
            'total_afectadas': total_afectadas,  # Para mantener compatibilidad con el código existente
            'cambio': 1 if total_afectadas > 0 else 0,  # Para Node-RED
            'ids': ids  # Para Node-RED
        }

    except Exception as e:
        logger.error(f"Error en merge_eventos_sql: {e}")
        print(f"❌ ERROR: {e}")
        return {'total_afectadas': 0,'cambio': 0,'ids': []}

def procesar_trayecto(evento_id):
    """
    Procesa un trayecto específico
    """
    try:
        df = obtener_eventos_por_ids(evento_id)
        if df is None:
            return {'success': False, 'error': 'No se encontró el evento en SQL'}
        
        df_influx = obtener_eventos_influx(df)
        if df_influx is None or df_influx.empty:
            return {'success': False, 'error': 'No se obtuvieron datos de InfluxDB'}
        
        success = actualizar_evento_con_trayecto(evento_id, df_influx.iloc[0])
        if success:
            return {'success': True, 'mensaje': 'Trayecto procesado correctamente'}
        else:
            return {'success': False, 'error': 'Error al actualizar la base de datos'}
            
    except Exception as e:
        logger.error(f"Error procesando trayecto {evento_id}: {e}")
        return {'success': False, 'error': str(e)}

# --- API FASTAPI ---
from fastapi import FastAPI, HTTPException, Query

app = FastAPI(title="API de Eventos", version="1.0.0")

@app.middleware("http")
async def log_requests(request, call_next):
    if request.url.path == "/event_data/":
        print(f"GET request: {dict(request.query_params)}")
    response = await call_next(request)
    return response

@app.get("/")
def root():
    return {"status": "ok"}

@app.get("/event_data/")
def procesar_evento(
    evento: str = Query(...),
    barco: str = Query(...),
    fecha: str = Query(...)
):
    try:
        # Validar formato de fecha (acepta tanto YYYY-MM-DD como ISO datetime)
        if 'T' in fecha:
            datetime.fromisoformat(fecha.replace('Z', '+00:00'))
        else:
            datetime.strptime(fecha, '%Y-%m-%d')
        
        resultado = main(fecha=fecha, barco=barco)
        
        if isinstance(resultado, tuple):
            df_resultado, merge_resultado = resultado
            
            return {"cambio": merge_resultado.get('cambio', 0),
                "ids": merge_resultado.get('ids', [])}
        else:
            raise HTTPException(status_code=500, detail="Error interno")
            
    except ValueError:
        raise HTTPException(status_code=400, detail="Fecha inválida")
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        # EN CASO DE ERROR, DEVOLVER SIN CAMBIOS
        return {"cambio": 0, "ids": []}
    
@app.get("/trayecto/")
def procesar_trayecto_endpoint(
    evento_id: int = Query(..., description="ID del evento a procesar")):
    """
    Procesa un trayecto específico
    
    Retorna:
    - success: true/false
    - evento_id: ID procesado
    - error: mensaje de error (si falló)
    """
    try:
        resultado = procesar_trayecto(evento_id)
        
        if resultado['success']:
            return {
                "success": True,
                "evento_id": evento_id
            }
        else:
            return {
                "success": False,
                "evento_id": evento_id,
                "error": resultado['error']
            }
            
    except Exception as e:
        logger.error(f"Error en endpoint trayecto: {str(e)}")
        return {
            "success": False,
            "evento_id": evento_id,
            "error": str(e)
        }

@app.get("/health")
def health():
    return {"status": "ok"}

if __name__ == "__main__":
   import uvicorn
   # USAR VARIABLES DE ENTORNO PARA UVICORN
   api_host = os.getenv('API_HOST', '0.0.0.0')
   api_port = int(os.getenv('API_PORT', 8002))
   uvicorn.run(app, host=api_host, port=api_port)
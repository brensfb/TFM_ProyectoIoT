from datetime import datetime, timedelta, timezone
import pandas as pd
import logging
from sqlalchemy import create_engine, text
from fastapi import FastAPI, HTTPException, Query, Body
from typing import List
from influxdb_client import InfluxDBClient
from influxdb_client.client.warnings import MissingPivotFunction
import warnings
import logging
import os
from dotenv import load_dotenv
warnings.simplefilter("ignore", MissingPivotFunction)

# Configurar logging
logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = FastAPI(title="API de Procesamiento de Eventos", version="1.0.0")

def obtener_eventos_por_ids(id):
    try:
        sql_server = os.getenv('SQL_SERVER')
        sql_username = os.getenv('SQL_USERNAME')
        sql_password = os.getenv('SQL_PASSWORD')
        sql_driver = os.getenv('SQL_DRIVER')
        conn_rms = create_engine(f"mssql+pyodbc://{sql_username}:{sql_password}@{sql_server}/rms?driver={sql_driver.replace(' ', '+')}&TrustServerCertificate=yes")
        
        sql_query = f"""
        ; WITH actual AS (
            SELECT barco, fechaDetectada
            FROM [Mars].[dbo].[eventos]
            WHERE id = '{id}' AND fechaDetectada IS NOT NULL)

        SELECT top 2 a.id, a.barco, a.tipoEvento, a.fechaDetectada, a.puerto
        FROM [Mars].[dbo].[eventos] a
        LEFT JOIN actual b ON a.barco = b.barco 
        WHERE a.fechaDetectada IS NOT NULL and a.barco = b.barco and a.fechaDetectada <= b.fechaDetectada
        ORDER BY a.fechaDetectada DESC
        """         
        df_evento = pd.read_sql(sql_query, conn_rms)

        if len(df_evento) == 2:
            evento_actual = df_evento.iloc[0] 
            evento_anterior = df_evento.iloc[1]
            df = {}
            
            if evento_actual['tipoEvento'] != evento_anterior['tipoEvento']:              
                df['fecha_fin'] = pd.to_datetime(evento_actual['fechaDetectada'])
                df['fecha_inicio'] = pd.to_datetime(evento_anterior['fechaDetectada'])
                df['barco'] = evento_actual['barco'].strip()
                df['evento'] = evento_actual['tipoEvento'].strip()
                df['puerto_fin'] = evento_actual['puerto'].strip()
                df['puerto_inicio'] = evento_anterior['puerto'].strip()
                if df['fecha_fin'].tz is None:
                    df['fecha_fin'] = df['fecha_fin'].tz_localize('UTC')
                if df['fecha_inicio'].tz is None:
                    df['fecha_inicio'] = df['fecha_inicio'].tz_localize('UTC')                
                return df
                                     
            else:
                print(f" ID {id}: Mismo tipo de evento {evento_actual['tipoEvento']}")
                return None
        else:
            print(f" ID {id}: No encontrado o no tiene evento anterior")
            return None

    except Exception as e:
        logger.error(f"Error al consultar eventos: {e}")
        print(f" Error con ID {id}: {e}")
        return None
    
def obtener_eventos_influx(df):
    try:
        if df is None:
            print(" Datos insuficientes para consulta InfluxDB")
            return None
        influx_url = os.getenv('INFLUX_URL')
        influx_token = os.getenv('INFLUX_TOKEN')
        influx_org = os.getenv('INFLUX_ORG')
        influx_bucket = os.getenv('INFLUX_BUCKET')

        client = InfluxDBClient(url=influx_url, token=influx_token, org=influx_org, timeout=600000)
        query_api = client.query_api()
        fecha_fin = df['fecha_fin']
        fecha_ini= df['fecha_inicio']
        barco = df['barco']
        evento = df['evento']
        puerto_inicio = df['puerto_inicio']
        puerto_fin = df['puerto_fin']

        # Asegurar UTC
        if fecha_ini.tz is None:
            fecha_ini = fecha_ini.replace(tzinfo=timezone.utc)
        if fecha_fin.tz is None:
            fecha_fin = fecha_fin.replace(tzinfo=timezone.utc)
        # Formatear para InfluxDB (ISO con Z al final)
        fecha_ini_str = fecha_ini.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        fecha_fin_str = fecha_fin.strftime('%Y-%m-%dT%H:%M:%S.%fZ') 

        # Construir nombre del barco para InfluxDB
        barco_influx = f"forpi{barco.lower()}"

        flux_query = f"""
        import "contrib/bonitoo-io/hex"
        import "date"
        import "strings"
        import "experimental/geo"
        import "timezone"
        import "array"

        barco = "{barco_influx}"
        puertoInicio = "{puerto_inicio}"
        puertoFin = "{puerto_fin}"
        evento = "{evento}"

        fin = time(v: "{fecha_fin_str}")
        inicio = time(v: "{fecha_ini_str}")

        eventosConHoras = array.from(rows: [
            {{host: barco,
                HoraInicio: inicio,
                HoraFin: fin,
                puertoInicio: puertoInicio,
                puertoFin: puertoFin,
                evento: evento
            }}
        ])

        
        eventosConMetricas = eventosConHoras
        |> map(fn: (r) => {{
        
            barco = r.host
            hourFactor = float(v: 60 * 60)
        
            tanquesData = from(bucket: "IoV")
                |> range(start: r.HoraInicio, stop: r.HoraFin)
                |> filter(fn: (r) => r["_measurement"] == "fuel_tanks")
                |> filter(fn: (r) => r["_field"] == "lts")
                |> filter(fn: (r) => r["host"] == barco)
                |> aggregateWindow(every: 1s, fn: last, createEmpty: false)
                |> group(columns: ["_time"], mode:"by")
                |> sum()
                |> group()
        
            tanquesInicio = tanquesData |> first() |> findRecord(fn: (key) => true, idx: 0)
            tanquesFinal = tanquesData |> last() |> findRecord(fn: (key) => true, idx: 0)
            
            fuelConsMmppLts = from(bucket: "IoV")
                |> range(start: r.HoraInicio, stop: r.HoraFin)
                |> filter(fn: (r) => r["_measurement"] == "main_engines")
                |> filter(fn: (r) => r["_field"] == "fuel_cons_lts")
                |> filter(fn: (r) => r["host"] == barco)
                |> drop(columns: ["host","topic","engine_id"])
                |> aggregateWindow(every: 1s, fn: mean, createEmpty: false)
                |> group(columns: ["_time"], mode:"by")
                |> sum()
                |> group()
                |> elapsed()
                |> map(fn: (r) => ({{r with _value: r._value * float(v: r.elapsed) / hourFactor}}))
                |> drop(columns: ["elapsed"])
                |> cumulativeSum()
                |> last()
                |> findRecord(fn: (key) => true, idx: 0)
        
            fuelConsMmppKg = from(bucket: "IoV")
                |> range(start: r.HoraInicio, stop: r.HoraFin)
                |> filter(fn: (r) => r["_measurement"] == "main_engines")
                |> filter(fn: (r) => r["_field"] == "fuel_cons_kg")
                |> filter(fn: (r) => r["host"] == barco)
                |> drop(columns: ["host","topic","engine_id"])
                |> aggregateWindow(every: 1s, fn: mean, createEmpty: false)
                |> group(columns: ["_time"], mode:"by")
                |> sum()
                |> group()
                |> elapsed()
                |> map(fn: (r) => ({{r with _value: r._value * float(v: r.elapsed) / hourFactor}}))
                |> drop(columns: ["elapsed"])
                |> cumulativeSum()
                |> last()
                |> findRecord(fn: (key) => true, idx: 0)
        
            fuelConsMmaaLts = from(bucket: "IoV")
                |> range(start: r.HoraInicio, stop: r.HoraFin)
                |> filter(fn: (r) => r["_measurement"] == "aux_engines")
                |> filter(fn: (r) => r["_field"] == "fuel_cons_lts")
                |> filter(fn: (r) => r["host"] == barco)
                |> drop(columns: ["host","topic","engine_id"])
                |> aggregateWindow(every: 1s, fn: mean, createEmpty: false)
                |> group(columns: ["_time"], mode:"by")
                |> sum()
                |> group()
                |> elapsed()
                |> map(fn: (r) => ({{r with _value: r._value * float(v: r.elapsed) / hourFactor}}))
                |> drop(columns: ["elapsed"])
                |> cumulativeSum()
                |> last()
                |> findRecord(fn: (key) => true, idx: 0)
        
            fuelConsMmaaKg = from(bucket: "IoV")
                |> range(start: r.HoraInicio, stop: r.HoraFin)
                |> filter(fn: (r) => r["_measurement"] == "aux_engines")
                |> filter(fn: (r) => r["_field"] == "fuel_cons_kg")
                |> filter(fn: (r) => r["host"] == barco)
                |> drop(columns: ["host","topic","engine_id"])
                |> aggregateWindow(every: 1s, fn: mean, createEmpty: false)
                |> group(columns: ["_time"], mode:"by")
                |> sum()
                |> group()
                |> elapsed()
                |> map(fn: (r) => ({{r with _value: r._value * float(v: r.elapsed) / hourFactor}}))
                |> drop(columns: ["elapsed"])
                |> cumulativeSum()
                |> last()
                |> findRecord(fn: (key) => true, idx: 0)
        
            kwh = from(bucket: "IoV")
                |> range(start: r.HoraInicio, stop: r.HoraFin)
                |> filter(fn: (r) => r["_measurement"] == "shorepower")
                |> filter(fn: (r) => r["_field"] == "kw")
                |> filter(fn: (r) => r["host"] == barco)
                |> drop(columns: ["host","topic"])
                |> aggregateWindow(every: 1s, fn: mean, createEmpty: false)
                |> group(columns: ["_time"], mode:"by")
                |> sum()
                |> group()
                |> elapsed(unit: 1s)
                |> map(fn: (r) => ({{r with _value: r._value * float(v: r.elapsed) / hourFactor}}))
                |> drop(columns: ["elapsed"])
                |> cumulativeSum()
                |> last()
                |> findRecord(fn: (key) => true, idx: 0)
        
            bunkering = from(bucket: "IoV")
                |> range(start: r.HoraInicio, stop: r.HoraFin)
                |> filter(fn: (r) => r["_measurement"] == "bunker")
                |> filter(fn: (r) => r["_field"] == "mass_total" or r["_field"] == "volume_total")
                |> filter(fn: (r) => r["host"] == barco)
                |> drop(columns: ["host","topic"])
                |> aggregateWindow(every: 1s, fn: mean, createEmpty: false)
                |> spread()
                |> pivot(rowKey:["_measurement"], columnKey: ["_field"], valueColumn: "_value")
                |> findRecord(fn: (key) => true, idx: 0)
        
            bunkeringEvento = from(bucket: "IoV")
                |> range(start: r.HoraInicio, stop: r.HoraFin)
                |> filter(fn: (r) => r["_measurement"] == "bunker")
                |> filter(fn: (r) => r["_field"] == "mass_flow" or r["_field"] == "volume_flow")
                |> filter(fn: (r) => r["host"] == barco)
                |> filter(fn: (r) => r["_value"] > 5000)
                |> aggregateWindow(every: 1s, fn: mean, createEmpty: false)
                |> group(columns: ["_time"], mode:"by")
                |> sum()
                |> group()
        
            bunkeringInicio = bunkeringEvento|> first()|> findRecord(fn: (key) => true, idx: 0)
            bunkeringFin = bunkeringEvento|> last()|> findRecord(fn: (key) => true, idx: 0)
        
            bunkeringTempDensity = from(bucket: "IoV")
                |> range(start: if exists bunkeringInicio._time then date.sub(from:bunkeringInicio._time,d:1m) else date.sub(from:r.HoraInicio,d:1m)
                    , stop: if exists bunkeringFin._time then date.add(to:bunkeringFin._time, d:1m) else date.add(to:r.HoraFin, d:1m))
                |> filter(fn: (r) => r["_measurement"] == "bunker")
                |> filter(fn: (r) => r["_field"] == "temperature" or r["_field"] == "density")
                |> filter(fn: (r) => r["host"] == barco)
                |> drop(columns: ["host","topic"])
                |> aggregateWindow(every: 1s, fn: last, createEmpty: false)
                |> quantile(q: 0.7)
                |> group()
                |> pivot(rowKey:["_measurement"], columnKey: ["_field"], valueColumn: "_value")
                |> findRecord(fn: (key) => true, idx: 0)
        
            distancia = from(bucket: "IoV")
                |> range(start: date.sub(from: r.HoraInicio, d: 30s), stop: date.add(to: r.HoraFin, d: 30s))
                |> filter(fn: (r) => r["_measurement"] == "navigation_v2")
                |> filter(fn: (r) => r["_field"] == "longitude" or r["_field"] == "latitude")
                |> filter(fn: (r) => r["host"] == barco)
                |> aggregateWindow(every: 10s, fn: mean, createEmpty: false)
                |> geo.shapeData(latField: "latitude", lonField: "longitude", level: 5)
                |> group(columns: ["id"])
                |> geo.totalDistance()
                |> map(fn: (r) => ({{r with _value: r._value / 1.852}}))
                |> findRecord(fn: (key) => true, idx: 0)
        
            loc = timezone.location(name: "Atlantic/Canary")

            return {{
                fum_utc: now(),
                //fecha: strings.substring(v: string(v: r.HoraInicio), start: 0, end: 10),
                Evento: r.evento,
                inicio_utc:r.HoraInicio,
                //FechaHoraInicio: date.time(t: r.HoraInicio, location: loc),
                fin_utc: r.HoraFin,
                //FechaHoraFin: date.time(t: r.HoraFin, location: loc),
                puerto_inicio: r.puertoInicio,
                puerto_fin: r.puertoFin,
                Trayecto: r.puertoInicio + r.puertoFin,
                duracion_segundos: float(v: int(v: r.HoraFin) - int(v: r.HoraInicio)) / 1000000000.0,
                distancia_nm: distancia._value,
                barco: strings.substring(v:strings.toUpper(v: barco), start: strings.strlen(v:barco) - 3, end: strings.strlen(v:barco)),
                tanques_inicio_lts: tanquesInicio._value,
                tanques_final_lts: tanquesFinal._value,
                fuel_cons_mmpp_lts: fuelConsMmppLts._value,
                fuel_cons_mmaa_lts: fuelConsMmaaLts._value,
                fuel_cons_mmpp_kg: fuelConsMmppKg._value,
                fuel_cons_mmaa_kg: fuelConsMmaaKg._value,
                shorepower_kwh: kwh._value,
                bunker_volume_total: bunkering.volume_total,
                bunker_mass_total: bunkering.mass_total,
                bunker_temperature: bunkeringTempDensity.temperature,
                bunker_density: bunkeringTempDensity.density  
            }}
        }})

        eventosConMetricas
        """
        
        result = query_api.query(flux_query)
        
        # Procesar resultados de InfluxDB
        records = []
        for table in result:
            for record in table.records:
                records.append(record.values)
        
        if records:
            df_influx = pd.DataFrame(records)
            return df_influx       
            
        else:
            print("❌ No se encontraron datos en InfluxDB")
            return pd.DataFrame()

    except Exception as e:
        logger.error(f"Error al consultar InfluxDB: {e}")
        print(f"❌ Error InfluxDB: {e}")
        return pd.DataFrame()

def actualizar_evento_con_trayecto(evento_id, datos_influx):
    """
    Actualiza los campos de trayecto en la tabla eventos con datos de InfluxDB
    """
    try:
        sql_server = os.getenv('SQL_SERVER')
        sql_username = os.getenv('SQL_USERNAME')
        sql_password = os.getenv('SQL_PASSWORD')
        sql_driver = os.getenv('SQL_DRIVER')
        conn_rms = create_engine(f"mssql+pyodbc://{sql_username}:{sql_password}@{sql_server}/rms?driver={sql_driver.replace(' ', '+')}&TrustServerCertificate=yes")
        
        # Mapear campos de InfluxDB a campos de la tabla
        campos_update = {
            'trayecto': datos_influx.get('Trayecto', ''),
            'fechaInicioDetectada': pd.to_datetime(datos_influx.get('inicio_utc')).replace(tzinfo=None) if datos_influx.get('inicio_utc') else None,
            'distanciaNm': datos_influx.get('distancia_nm', 0),
            'duracionSegundos': int(datos_influx.get('duracion_segundos', 0)),
            'tanquesInicioLts': datos_influx.get('tanques_inicio_lts', 0),
            'tanquesFinalLts': datos_influx.get('tanques_final_lts', 0),
            'fuelConsMmppLts': datos_influx.get('fuel_cons_mmpp_lts', 0),
            'fuelConsMmaaLts': datos_influx.get('fuel_cons_mmaa_lts', 0),
            'fuelConsMmppKg': datos_influx.get('fuel_cons_mmpp_kg', 0),
            'fuelConsMmaaKg': datos_influx.get('fuel_cons_mmaa_kg', 0),
            'shorepowerKwh': datos_influx.get('shorepower_kwh', 0),
            'bunkerVolumeTotal': datos_influx.get('bunker_volume_total', 0),
            'bunkerMassTotal': datos_influx.get('bunker_mass_total', 0),
            'bunkerTemperatura': datos_influx.get('bunker_temperature', 0),
            'bunkerDensidad': datos_influx.get('bunker_density', 0),
            'fumTrayecto': datetime.now()
        }
        
        # Construir la query de UPDATE
        datos = []
        for campo, valor in campos_update.items():
            if valor is not None:
                if isinstance(valor, str):
                    datos.append(f"{campo} = '{valor}'")
                elif isinstance(valor, datetime):
                    datos.append(f"{campo} = '{valor.strftime('%Y%m%d %H:%M:%S')}'")
                else:
                    datos.append(f"{campo} = {valor}")
        
        update_query = f"""
        UPDATE [Mars].[dbo].[eventos] 
        SET {', '.join(datos)}
        WHERE id = {evento_id}
        """
        # Ejecutar la actualización
        with conn_rms.connect() as connection:
            result = connection.execute(text(update_query))
            connection.commit()
        
        print(f" Evento ID {evento_id} actualizado correctamente")
        return True
        
    except Exception as e:
        logger.error(f"Error actualizando evento: {e}")
        print(f"❌ Error actualizando evento ID {evento_id}: {e}")
        return False

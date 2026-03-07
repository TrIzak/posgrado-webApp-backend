import axios from 'axios';
import * as crypto from 'crypto';
import * as sql from 'mssql';
import * as dotenv from 'dotenv';
import cron from 'node-cron';

// Cargar variables de entorno
dotenv.config();

// Configuración de base de datos
const dbConfig: sql.config = {
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_DATABASE,
    server: process.env.DB_SERVER || 'localhost',
    port: parseInt(process.env.DB_PORT || '1433'),
    options: {
        encrypt: true, 
        trustServerCertificate: true, 
	instanceName: process.env.DB_INSTANCE, 
    },
};

// Funciones de Conversión de Unidades
const fToC = (f: number): number => Number(((f - 32) * (5 / 9)).toFixed(2));
const inHgToHpa = (inHg: number): number => Number((inHg * 33.8639).toFixed(2));
const mphToKmh = (mph: number): number => Number((mph * 1.60934).toFixed(2));

// Función para generar la firma de la API de Davis
function generateDavisSignature(timestamp: number): string {
    const apiKey = process.env.DAVIS_API_KEY!;
    const apiSecret = process.env.DAVIS_API_SECRET!;
    const stationId = process.env.STATION_ID!;

    const stringToHash = `api-key${apiKey}station-id${stationId}t${timestamp}`;
    
    return crypto
        .createHmac('sha256', apiSecret)
        .update(stringToHash)
        .digest('hex');
}

// Lógica principal de ingesta
async function ingestWeatherData() {
    console.log(`\n[${new Date().toISOString()}] Iniciando ciclo de ingesta...`);
    let pool: sql.ConnectionPool | null = null;

    try {
        const timestamp = Math.floor(Date.now() / 1000);
        const apiSignature = generateDavisSignature(timestamp);
      // 1. Petición HTTP a WeatherLink Cloud (URL Corregida)
        const stationId = process.env.STATION_ID;
        const response = await axios.get(`https://api.weatherlink.com/v2/current/${stationId}`, {
            params: {
                'api-key': process.env.DAVIS_API_KEY,
                't': timestamp,
                'api-signature': apiSignature
            }
        });

        const data = response.data;
        const sensors = data.sensors || [];
        const weatherSensor = sensors.find((s: any) => s.data_structure_type === 2);
        const airLinkSensor = sensors.find((s: any) => s.data_structure_type === 120);

        if (!weatherSensor || !weatherSensor.data || weatherSensor.data.length === 0) {
            throw new Error("No se encontraron datos meteorológicos válidos en la respuesta.");
        }

        const wData = weatherSensor.data[0];
        const aData = airLinkSensor && airLinkSensor.data ? airLinkSensor.data[0] : null;

        const dateObj = new Date(wData.ts * 1000);

        // 2. Conexión y guardado en SQL Server
        pool = await sql.connect(dbConfig);
        const request = pool.request();

        request.input('ts', sql.DateTime2, dateObj);
        request.input('stationId', sql.Int, data.station_id);
        request.input('tOut', sql.Float, fToC(wData.temp_out));
        request.input('tIn', sql.Float, fToC(wData.temp_in));
        request.input('wChill', sql.Float, fToC(wData.wind_chill));
        request.input('hIndex', sql.Float, fToC(wData.heat_index));
        request.input('dPoint', sql.Float, fToC(wData.dew_point));
        request.input('hOut', sql.Float, wData.hum_out);
        request.input('hIn', sql.Float, wData.hum_in);
        request.input('baro', sql.Float, inHgToHpa(wData.bar));
        request.input('wSpeed', sql.Float, mphToKmh(wData.wind_speed));
        request.input('wDir', sql.Int, wData.wind_dir);
        request.input('rRate', sql.Float, wData.rain_rate_mm);
        request.input('rTotal', sql.Float, wData.rain_year_mm); 
        request.input('pm25', sql.Float, aData ? aData.pm_2p5 : null);
        request.input('pm10', sql.Float, aData ? aData.pm_10 : null);
        request.input('aqi', sql.Int, aData ? aData.aqi : null);

        const query = `
            INSERT INTO Lecturas_Meteorologicas (
                timestamp_lectura, station_id, temp_out_c, temp_in_c, wind_chill_c, 
                heat_index_c, dew_point_c, hum_out, hum_in, barometer_hpa, 
                wind_speed_kmh, wind_dir, rain_rate_mm, rain_total_mm, pm2_5, pm10, aqi
            ) VALUES (
                @ts, @stationId, @tOut, @tIn, @wChill, 
                @hIndex, @dPoint, @hOut, @hIn, @baro, 
                @wSpeed, @wDir, @rRate, @rTotal, @pm25, @pm10, @aqi
            )
        `;

        await request.query(query);
        console.log(`[${dateObj.toISOString()}] ✅ Datos insertados correctamente.`);

    } catch (error: any) {
        // Manejo inteligente de errores
        if (error && error.number === 2627) {
            console.log(`[${new Date().toISOString()}] ⏩ Omitido: La lectura para este minuto ya fue registrada previamente.`);
        } else {
            console.error(`[${new Date().toISOString()}] ❌ Error crítico:`, error.message);
        }
    } finally {
        if (pool) {
            await pool.close();
        }
    }
}

// Programar la ejecución cada 5 minutos
cron.schedule('*/5 * * * *', () => {
    ingestWeatherData();
});

console.log('🚀 Worker iniciado. Ejecutando primera ingesta...');
ingestWeatherData();
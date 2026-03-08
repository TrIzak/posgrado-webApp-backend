import axios from 'axios';
import * as crypto from 'crypto';
import { Pool } from 'pg';
import * as dotenv from 'dotenv';
import cron from 'node-cron';

dotenv.config();

// Configuración de PostgreSQL (Supabase)
const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false } // Requerido para conexiones a Supabase
});

const fToC = (f: number): number => Number(((f - 32) * (5 / 9)).toFixed(2));
const inHgToHpa = (inHg: number): number => Number((inHg * 33.8639).toFixed(2));
const mphToKmh = (mph: number): number => Number((mph * 1.60934).toFixed(2));

function generateDavisSignature(timestamp: number): string {
    const apiKey = process.env.DAVIS_API_KEY!;
    const apiSecret = process.env.DAVIS_API_SECRET!;
    const stationId = process.env.STATION_ID!;
    const stringToHash = `api-key${apiKey}station-id${stationId}t${timestamp}`;
    return crypto.createHmac('sha256', apiSecret).update(stringToHash).digest('hex');
}

async function ingestWeatherData() {
    console.log(`\n[${new Date().toISOString()}] Iniciando ciclo de ingesta a Supabase...`);
    
    try {
        const timestamp = Math.floor(Date.now() / 1000);
        const apiSignature = generateDavisSignature(timestamp);
        const stationId = process.env.STATION_ID;
        
        // 1. Fetch a Davis
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
            throw new Error("No se encontraron datos meteorológicos válidos.");
        }

        const wData = weatherSensor.data[0];
        const aData = airLinkSensor && airLinkSensor.data ? airLinkSensor.data[0] : null;
        const dateObj = new Date(wData.ts * 1000);

        // 2. Query para PostgreSQL con $1, $2, etc.
        const query = `
            INSERT INTO lecturas_meteorologicas (
                timestamp_lectura, station_id, temp_out_c, temp_in_c, wind_chill_c, 
                heat_index_c, dew_point_c, hum_out, hum_in, barometer_hpa, 
                wind_speed_kmh, wind_dir, rain_rate_mm, rain_total_mm, pm2_5, pm10, aqi
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17
            )
        `;

        // 3. Valores a insertar
        const values = [
            dateObj.toISOString(), data.station_id, fToC(wData.temp_out), fToC(wData.temp_in), 
            fToC(wData.wind_chill), fToC(wData.heat_index), fToC(wData.dew_point), 
            wData.hum_out, wData.hum_in, inHgToHpa(wData.bar), mphToKmh(wData.wind_speed), 
            wData.wind_dir, wData.rain_rate_mm, wData.rain_year_mm, 
            aData ? aData.pm_2p5 : null, aData ? aData.pm_10 : null, aData ? aData.aqi : null
        ];

        // 4. Ejecutar en Supabase
        await pool.query(query, values);
        console.log(`[${dateObj.toISOString()}] ✅ Datos insertados correctamente en Supabase.`);

    } catch (error: any) {
        // Código 23505 en Postgres es "Unique Violation" (Dato duplicado)
        if (error.code === '23505') {
            console.log(`[${new Date().toISOString()}] ⏩ Omitido: La lectura para este minuto ya existe en Supabase.`);
        } else {
            console.error(`[${new Date().toISOString()}] ❌ Error crítico:`, error.message);
        }
    }
}

// Programar la ejecución cada 15 minutos
cron.schedule('*/15 * * * *', () => {
    ingestWeatherData();
});

console.log('🚀 Worker PostgreSQL iniciado. Ejecutando primera ingesta...');
ingestWeatherData();
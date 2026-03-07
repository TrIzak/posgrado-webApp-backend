"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g = Object.create((typeof Iterator === "function" ? Iterator : Object).prototype);
    return g.next = verb(0), g["throw"] = verb(1), g["return"] = verb(2), typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (g && (g = 0, op[0] && (_ = 0)), _) try {
            if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [op[0] & 2, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
};
Object.defineProperty(exports, "__esModule", { value: true });
var axios_1 = require("axios");
var crypto = require("crypto");
var sql = require("mssql");
var dotenv = require("dotenv");
var node_cron_1 = require("node-cron");
// Cargar variables de entorno
dotenv.config();
// Configuración de base de datos
var dbConfig = {
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
var fToC = function (f) { return Number(((f - 32) * (5 / 9)).toFixed(2)); };
var inHgToHpa = function (inHg) { return Number((inHg * 33.8639).toFixed(2)); };
var mphToKmh = function (mph) { return Number((mph * 1.60934).toFixed(2)); };
// Función para generar la firma de la API de Davis
function generateDavisSignature(timestamp) {
    var apiKey = process.env.DAVIS_API_KEY;
    var apiSecret = process.env.DAVIS_API_SECRET;
    var stationId = process.env.STATION_ID;
    var stringToHash = "api-key".concat(apiKey, "station-id").concat(stationId, "t").concat(timestamp);
    return crypto
        .createHmac('sha256', apiSecret)
        .update(stringToHash)
        .digest('hex');
}
// Lógica principal de ingesta
function ingestWeatherData() {
    return __awaiter(this, void 0, void 0, function () {
        var pool, timestamp, apiSignature, response, data, sensors, weatherSensor, airLinkSensor, wData, aData, dateObj, request, query, error_1;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    console.log("\n[".concat(new Date().toISOString(), "] Iniciando ciclo de ingesta..."));
                    pool = null;
                    _a.label = 1;
                case 1:
                    _a.trys.push([1, 5, 6, 9]);
                    timestamp = Math.floor(Date.now() / 1000);
                    apiSignature = generateDavisSignature(timestamp);
                    return [4 /*yield*/, axios_1.default.get('https://api.weatherlink.com/v2/current', {
                            params: {
                                'station-id': process.env.STATION_ID,
                                'api-key': process.env.DAVIS_API_KEY,
                                't': timestamp,
                                'api-signature': apiSignature
                            }
                        })];
                case 2:
                    response = _a.sent();
                    data = response.data;
                    sensors = data.sensors || [];
                    weatherSensor = sensors.find(function (s) { return s.data_structure_type === 2; });
                    airLinkSensor = sensors.find(function (s) { return s.data_structure_type === 120; });
                    if (!weatherSensor || !weatherSensor.data || weatherSensor.data.length === 0) {
                        throw new Error("No se encontraron datos meteorológicos válidos en la respuesta.");
                    }
                    wData = weatherSensor.data[0];
                    aData = airLinkSensor && airLinkSensor.data ? airLinkSensor.data[0] : null;
                    dateObj = new Date(wData.ts * 1000);
                    return [4 /*yield*/, sql.connect(dbConfig)];
                case 3:
                    // 2. Conexión y guardado en SQL Server
                    pool = _a.sent();
                    request = pool.request();
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
                    query = "\n            INSERT INTO Lecturas_Meteorologicas (\n                timestamp_lectura, station_id, temp_out_c, temp_in_c, wind_chill_c, \n                heat_index_c, dew_point_c, hum_out, hum_in, barometer_hpa, \n                wind_speed_kmh, wind_dir, rain_rate_mm, rain_total_mm, pm2_5, pm10, aqi\n            ) VALUES (\n                @ts, @stationId, @tOut, @tIn, @wChill, \n                @hIndex, @dPoint, @hOut, @hIn, @baro, \n                @wSpeed, @wDir, @rRate, @rTotal, @pm25, @pm10, @aqi\n            )\n        ";
                    return [4 /*yield*/, request.query(query)];
                case 4:
                    _a.sent();
                    console.log("[".concat(dateObj.toISOString(), "] \u2705 Datos insertados correctamente."));
                    return [3 /*break*/, 9];
                case 5:
                    error_1 = _a.sent();
                    // Manejo inteligente de errores
                    if (error_1 && error_1.number === 2627) {
                        console.log("[".concat(new Date().toISOString(), "] \u23E9 Omitido: La lectura para este minuto ya fue registrada previamente."));
                    }
                    else {
                        console.error("[".concat(new Date().toISOString(), "] \u274C Error cr\u00EDtico:"), error_1.message);
                    }
                    return [3 /*break*/, 9];
                case 6:
                    if (!pool) return [3 /*break*/, 8];
                    return [4 /*yield*/, pool.close()];
                case 7:
                    _a.sent();
                    _a.label = 8;
                case 8: return [7 /*endfinally*/];
                case 9: return [2 /*return*/];
            }
        });
    });
}
// Programar la ejecución cada 5 minutos
node_cron_1.default.schedule('*/5 * * * *', function () {
    ingestWeatherData();
});
console.log('🚀 Worker iniciado. Ejecutando primera ingesta...');
ingestWeatherData();
